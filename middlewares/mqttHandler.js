const awsIot = require("aws-iot-device-sdk");
const MessageModel = require("../models/mqtt-message-model");
const AllTopicsModel = require("../models/all-mqtt-messages");
const Supervisor = require("../models/supervisor-model");
const Employee = require("../models/employee-model");
const sendMail = require("../utils/mail");
const NodeCache = require("node-cache");
const { EventEmitter } = require("events");

const BATCH_SIZE = 10;
const BATCH_INTERVAL = 1000;
const MAX_QUEUE_SIZE = 100;
const MAX_MAIL_RETRIES = 3;
const MAIL_RETRY_DELAY = 1000;

class EmailQueue extends EventEmitter {
  constructor() {
    super();
    this.queue = [];
    this.processing = false;
    this.processQueue();
  }

  async addToQueue(emailData) {
    this.queue.push({
      ...emailData,
      retries: 0,
      timestamp: Date.now(),
    });
    this.emit("mailAdded");
  }

  async processQueue() {
    if (this.processing || this.queue.length === 0) {
      setTimeout(() => this.processQueue(), 100);
      return;
    }

    this.processing = true;
    const currentTime = Date.now();

    try {
      const mailPromises = [];

      while (this.queue.length > 0) {
        const email = this.queue[0];

        if (email.retries >= MAX_MAIL_RETRIES) {
          console.error(
            `Failed to send email after ${MAX_MAIL_RETRIES} retries:`,
            email
          );
          this.queue.shift();
          continue;
        }

        if (
          email.timestamp + MAIL_RETRY_DELAY > currentTime &&
          email.retries > 0
        ) {
          break;
        }

        const currentEmail = this.queue.shift();
        mailPromises.push(
          this.sendMailWithRetry(currentEmail).catch((error) => {
            console.error("Email sending error:", error);
            currentEmail.retries++;
            currentEmail.timestamp = Date.now();
            this.queue.push(currentEmail);
          })
        );
      }

      await Promise.all(mailPromises);
    } finally {
      this.processing = false;
      setTimeout(() => this.processQueue(), 100);
    }
  }

  async sendMailWithRetry({ recipients, subject, message, retries }) {
    try {
      const sendPromises = recipients.map((recipient) =>
        sendMail(recipient, subject, message).catch((error) => {
          console.error(`Failed to send email to ${recipient}:`, error);
          throw error;
        })
      );

      await Promise.all(sendPromises);
      console.log(
        `Successfully sent emails to ${recipients.length} recipients`
      );
    } catch (error) {
      if (retries < MAX_MAIL_RETRIES) {
        throw error;
      }
      console.error("Max retries reached for email:", { recipients, subject });
    }
  }
}

class MQTTHandler {
  constructor() {
    this.messageQueue = new Map();
    this.latestMessages = new Map();
    this.subscribedTopics = new Set();
    this.thresholdStates = new Map();
    this.processingBatch = false;
    this.emailQueue = new EmailQueue();

    this.recipientsCache = new NodeCache({
      stdTTL: 3600,
      checkperiod: 600,
      useClones: false,
    });

    this.thresholdCache = new NodeCache({
      stdTTL: 1800,
      checkperiod: 300,
      useClones: false,
    });

    this.device = this.initializeDevice();
    this.initializeMessageBatchProcessing();
  }

  initializeDevice() {
    const device = awsIot.device({
      keyPath:
        process.env.AWS_KEY_PATH || "./AWS_DATA_CERTIFICATES/Private.key",
      certPath:
        process.env.AWS_CERT_PATH || "./AWS_DATA_CERTIFICATES/device.crt",
      caPath:
        process.env.AWS_CA_PATH || "./AWS_DATA_CERTIFICATES/AmazonRootCA1.pem",
      clientId: process.env.AWS_CLIENT_ID || "503561454502",
      host:
        process.env.AWS_IOT_HOST ||
        "a1uccysxn7j38q-ats.iot.ap-south-1.amazonaws.com",
      reconnectPeriod: 1000,
      keepalive: 30,
    });

    device.on("connect", () => {
      console.log("Connected to AWS IoT");
      this.resubscribeToTopics();
    });
    device.on("message", this.handleMessage.bind(this));
    device.on("error", (error) => console.error("MQTT Error:", error));
    device.on("offline", () => console.log("MQTT Client Offline"));
    device.on("reconnect", () => console.log("MQTT Client Reconnecting"));

    return device;
  }

  async resubscribeToTopics() {
    for (const topic of this.subscribedTopics) {
      this.device.subscribe(topic);
      console.log(`Resubscribed to topic: ${topic}`);
    }
  }

  initializeMessageBatchProcessing() {
    setInterval(async () => {
      if (this.processingBatch) return;
      try {
        this.processingBatch = true;
        await this.processBatch();
      } finally {
        this.processingBatch = false;
      }
    }, BATCH_INTERVAL);
  }

  async handleMessage(topic, payload) {
    try {
      const value = this.parsePayload(payload);
      if (value === null) return;

      this.updateLatestMessage(topic, value);
      this.queueMessage(topic, value);
      await this.checkThresholds(topic, value);
    } catch (error) {
      console.error("Message handling error:", error);
    }
  }

  updateLatestMessage(topic, value) {
    this.latestMessages.set(topic, {
      message: { message: value },
      timestamp: new Date(),
    });
  }

  queueMessage(topic, value) {
    if (!this.messageQueue.has(topic)) {
      this.messageQueue.set(topic, []);
    }

    const messages = this.messageQueue.get(topic);
    messages.push({ value, timestamp: Date.now() });

    if (messages.length > MAX_QUEUE_SIZE) {
      messages.splice(0, messages.length - MAX_QUEUE_SIZE);
    }
  }

  async processBatch() {
    const batchOperations = [];

    for (const [topic, messages] of this.messageQueue.entries()) {
      if (messages.length === 0) continue;

      const batch = messages.splice(0, BATCH_SIZE);

      if (batch.length > 0) {
        batchOperations.push(
          MessageModel.bulkWrite(
            batch.map(({ value, timestamp }) => ({
              updateOne: {
                filter: { topic },
                update: {
                  $push: {
                    messages: {
                      message: value,
                      timestamp: new Date(timestamp),
                    },
                  },
                },
                upsert: true,
              },
            }))
          )
        );
      }
    }

    if (batchOperations.length > 0) {
      await Promise.allSettled(batchOperations).then((results) => {
        results.forEach((result, index) => {
          if (result.status === "rejected") {
            console.error(`Batch operation ${index} failed:`, result.reason);
          }
        });
      });
    }
  }

  parsePayload(payload) {
    try {
      const data = typeof payload === "string" ? payload : payload.toString();
      const parsed = JSON.parse(data);
      return parsed.message?.message ?? parseFloat(data);
    } catch {
      const value = parseFloat(payload.toString());
      return isNaN(value) ? null : value;
    }
  }

  async getRecipients(topic) {
    const cached = this.recipientsCache.get(topic);
    if (cached) return cached;

    try {
      const [employees, supervisors] = await Promise.all([
        Employee.find({ topics: topic }).select("email").lean(),
        Supervisor.find({ topics: topic }).select("email").lean(),
      ]);

      const recipients = [
        ...new Set([
          ...employees.map((emp) => emp.email),
          ...supervisors.map((sup) => sup.email),
        ]),
      ];

      if (recipients.length > 0) {
        this.recipientsCache.set(topic, recipients);
      }

      return recipients;
    } catch (error) {
      console.error("Error fetching recipients:", error);
      return [];
    }
  }

  async checkThresholds(topic, liveValue) {
    const thresholds = await this.getThresholds(topic);
    if (!thresholds?.length) return;

    const topicState = this.thresholdStates.get(topic) || new Map();
    const alerts = [];

    for (const { color, value, resetValue } of thresholds) {
      const currentState = topicState.get(color) || false;

      if (liveValue >= value && !currentState) {
        topicState.set(color, true);
        alerts.push(
          this.prepareThresholdAlert(topic, { color, value }, liveValue)
        );
      } else if (liveValue < resetValue) {
        topicState.set(color, false);
      }
    }

    this.thresholdStates.set(topic, topicState);

    if (alerts.length > 0) {
      const recipients = await this.getRecipients(topic);
      if (recipients.length > 0) {
        await Promise.all(
          alerts.map((alert) =>
            this.emailQueue.addToQueue({
              recipients,
              subject: alert.subject,
              message: alert.message,
            })
          )
        );
      }
    }
  }

  async getThresholds(topic) {
    const cached = this.thresholdCache.get(topic);
    if (cached) return cached.thresholds;

    try {
      const topicData = await AllTopicsModel.findOne({ topic })
        .select("thresholds")
        .lean();
      if (topicData) {
        this.thresholdCache.set(topic, topicData);
        return topicData.thresholds;
      }
      return null;
    } catch (error) {
      console.error("Error fetching thresholds:", error);
      return null;
    }
  }

  prepareThresholdAlert(topic, threshold, liveValue) {
    return {
      subject: `${threshold.color === "red" ? "Danger" : "Warning"}: ${topic}`,
      message: `The value for ${topic} has crossed the ${
        threshold.color
      } threshold. Current value: ${liveValue}\n\nTimestamp: ${new Date().toISOString()}`,
    };
  }

  subscribeToTopic(topic) {
    if (!this.subscribedTopics.has(topic)) {
      this.device.subscribe(topic);
      this.subscribedTopics.add(topic);
      this.messageQueue.set(topic, []);
      console.log(`Subscribed to topic: ${topic}`);
    }
  }

  unsubscribeFromTopic(topic) {
    if (this.subscribedTopics.has(topic)) {
      this.device.unsubscribe(topic);
      this.subscribedTopics.delete(topic);
      this.messageQueue.delete(topic);
      this.latestMessages.delete(topic);
      this.thresholdStates.delete(topic);
      console.log(`Unsubscribed from topic: ${topic}`);
    }
  }

  getLatestLiveMessage(topic) {
    return this.latestMessages.get(topic) || null;
  }

  isTopicSubscribed(topic) {
    return this.subscribedTopics.has(topic);
  }
}

// Create singleton instance
const mqttHandler = new MQTTHandler();

module.exports = {
  subscribeToTopic: mqttHandler.subscribeToTopic.bind(mqttHandler),
  getLatestLiveMessage: mqttHandler.getLatestLiveMessage.bind(mqttHandler),
  isTopicSubscribed: mqttHandler.isTopicSubscribed.bind(mqttHandler),
  unsubscribeFromTopic: mqttHandler.unsubscribeFromTopic.bind(mqttHandler),
};

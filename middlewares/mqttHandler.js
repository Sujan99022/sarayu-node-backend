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
const THRESHOLD_COOLDOWN_PERIOD = 30000; // 30 seconds

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

  parsePayload(payload) {
    try {
      const payloadStr =
        typeof payload === "string" ? payload : payload.toString();

      try {
        const jsonParsed = JSON.parse(payloadStr);

        if (jsonParsed && typeof jsonParsed === "object") {
          if (jsonParsed.message && jsonParsed.message.message !== undefined) {
            return jsonParsed.message.message;
          }

          if (jsonParsed.message !== undefined) {
            const numValue = parseFloat(jsonParsed.message);
            return !isNaN(numValue) ? numValue : jsonParsed.message;
          }
        }

        const numValue = parseFloat(jsonParsed);
        return !isNaN(numValue) ? numValue : null;
      } catch (jsonError) {
        const numValue = parseFloat(payloadStr);
        return !isNaN(numValue) ? numValue : null;
      }
    } catch (error) {
      console.error("Payload parsing error:", error);
      return null;
    }
  }

  async handleMessage(topic, payload) {
    try {
      const value = this.parsePayload(payload);

      console.log(`Received message on topic ${topic}:`, {
        originalPayload: payload.toString(),
        parsedValue: value,
      });

      if (value === null) {
        console.warn(
          `Unable to parse payload for topic ${topic}:`,
          payload.toString()
        );
        return;
      }

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

    const sortedThresholds = [...thresholds].sort((a, b) => b.value - a.value);

    const topicState = this.thresholdStates.get(topic) || new Map();
    const currentTime = Date.now();
    let higherThresholdTriggered = false;
    for (const { color, value, resetValue } of sortedThresholds) {
      const stateKey = `${color}-${value}`;
      const currentState = topicState.get(stateKey) || {
        triggered: false,
        lastAlertTime: 0,
      };

      console.log(`Checking ${color} threshold for ${topic}:`, {
        liveValue,
        thresholdValue: value,
        resetValue,
        currentState,
      });

      if (liveValue >= value) {
        const cooldownPassed =
          currentTime - currentState.lastAlertTime >= THRESHOLD_COOLDOWN_PERIOD;

        if (!currentState.triggered || cooldownPassed) {
          console.log(`${color} threshold crossed for ${topic}`);
          if (higherThresholdTriggered) continue;

          topicState.set(stateKey, {
            triggered: true,
            lastAlertTime: currentTime,
          });

          const recipients = await this.getRecipients(topic);
          if (recipients.length > 0) {
            const alert = {
              recipients,
              ...this.prepareThresholdAlert(topic, { color, value }, liveValue),
            };
            await this.emailQueue.addToQueue(alert);

            higherThresholdTriggered = true;
            if (color === "red") {
              break;
            }
          }
        }
      } else if (liveValue < resetValue) {
        console.log(`Resetting ${color} threshold state for ${topic}`);
        topicState.set(stateKey, {
          triggered: false,
          lastAlertTime: 0,
        });
      }
    }

    this.thresholdStates.set(topic, topicState);
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
    const alertType = threshold.color === "red" ? "Danger" : "Warning";
    const severity = threshold.color === "red" ? "critical" : "warning";

    return {
      subject: `${alertType}: ${topic} Threshold Exceeded`,
      message: `
  ${alertType} Alert for ${topic}
  
  Current Value: ${liveValue}
  Threshold: ${threshold.value}
  Severity: ${severity}
  Timestamp: ${new Date().toISOString()}
  
  ${
    threshold.color === "red"
      ? "IMMEDIATE ACTION REQUIRED: Critical threshold exceeded!"
      : "WARNING: Monitor situation closely."
  }`,
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
    const message = this.latestMessages.get(topic);
    console.log(`Retrieving latest message for topic ${topic}:`, message);
    return message || null;
  }

  isTopicSubscribed(topic) {
    return this.subscribedTopics.has(topic);
  }
}

const mqttHandler = new MQTTHandler();

module.exports = {
  subscribeToTopic: mqttHandler.subscribeToTopic.bind(mqttHandler),
  getLatestLiveMessage: mqttHandler.getLatestLiveMessage.bind(mqttHandler),
  isTopicSubscribed: mqttHandler.isTopicSubscribed.bind(mqttHandler),
  unsubscribeFromTopic: mqttHandler.unsubscribeFromTopic.bind(mqttHandler),
};

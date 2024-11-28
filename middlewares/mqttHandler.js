const awsIot = require("aws-iot-device-sdk");
const MessageModel = require("../models/mqtt-message-model");
const AllTopicsModel = require("../models/all-mqtt-messages");
const Supervisor = require("../models/supervisor-model");
const Employee = require("../models/employee-model");
const sendMail = require("../utils/mail");

let latestMessages = {};
let subscribedTopics = new Set();
let thresholdStates = {}; // To track the state of threshold crossings for each topic

const device = awsIot.device({
  keyPath: "./AWS_DATA_CERTIFICATES/Private.key",
  certPath: "./AWS_DATA_CERTIFICATES/device.crt",
  caPath: "./AWS_DATA_CERTIFICATES/AmazonRootCA1.pem",
  clientId: "503561454502",
  host: "a1uccysxn7j38q-ats.iot.ap-south-1.amazonaws.com",
});

device.on("connect", () => {
  console.log("Connected to AWS IoT");
});

device.on("message", async (topic, payload) => {
  try {
    let liveValue;

    try {
      const messageData = JSON.parse(payload.toString());
      liveValue = messageData.message.message;
    } catch (e) {
      liveValue = parseFloat(payload.toString());
      console.log(`Received raw data for topic ${topic}:`, liveValue);
    }

    if (isNaN(liveValue)) {
      console.log("Received data is not a valid number:", payload.toString());
      return;
    }

    console.log(`Received live value for topic ${topic}: ${liveValue}`);

    const timestamp = new Date();
    latestMessages[topic] = { message: { message: liveValue }, timestamp };

    // Save to MongoDB (updated schema)
    await MessageModel.findOneAndUpdate(
      { topic },
      { $push: { messages: { message: liveValue, timestamp } } },
      { upsert: true, new: true }
    );

    // Fetch thresholds for the topic
    const topicThresholds = await AllTopicsModel.findOne({ topic });
    if (!topicThresholds) {
      console.log(`No thresholds found for topic ${topic}`);
      return;
    }

    // Initialize the threshold state for the topic if not already present
    if (!thresholdStates[topic]) {
      thresholdStates[topic] = { orange: false, red: false }; // false means threshold hasn't been crossed yet
    }

    topicThresholds.thresholds.forEach(async (threshold) => {
      if (
        threshold.color === "orange" &&
        liveValue >= threshold.value &&
        !thresholdStates[topic].orange
      ) {
        // orange threshold crossed for the first time
        thresholdStates[topic].orange = true; // Set orange threshold to true

        const employees = await Employee.find({ topics: topic });
        const supervisors = await Supervisor.find({ topics: topic });

        const allRecipients = [
          ...employees.map((emp) => emp.email),
          ...supervisors.map((sup) => sup.email),
        ];

        // Send warning email for orange threshold
        allRecipients.forEach((email) => {
          sendMail(
            email,
            `Warning: ${topic}`,
            `The value for ${topic} has crossed the yellow threshold. Current value: ${liveValue}`
          );
        });
        console.log("Warning mail sent for yellow threshold");
      } else if (
        threshold.color === "red" &&
        liveValue >= threshold.value &&
        !thresholdStates[topic].red
      ) {
        // Red threshold crossed for the first time
        thresholdStates[topic].red = true; // Set red threshold to true

        const employees = await Employee.find({ topics: topic });
        const supervisors = await Supervisor.find({ topics: topic });

        const allRecipients = [
          ...employees.map((emp) => emp.email),
          ...supervisors.map((sup) => sup.email),
        ];

        // Send danger email for red threshold
        allRecipients.forEach((email) => {
          sendMail(
            email,
            `Danger: ${topic}`,
            `The value for ${topic} has crossed the red threshold. Current value: ${liveValue}`
          );
        });
        console.log("Danger mail sent for red threshold");
      }
    });

    // If value falls below orange or red, reset the state so we can send emails when thresholds are crossed again
    if (liveValue < 60) {
      // Assuming 60 as the lower threshold for orange
      thresholdStates[topic].orange = false; // Reset orange threshold
    }
    if (liveValue < 80) {
      // Assuming 80 as the lower threshold for red
      thresholdStates[topic].red = false; // Reset red threshold
    }
  } catch (error) {
    console.error("Error processing message:", error);
  }
});

function subscribeToTopic(topic) {
  if (!subscribedTopics.has(topic)) {
    device.subscribe(topic);
    subscribedTopics.add(topic);
    console.log(`Subscribed to topic: ${topic}`);
  } else {
    console.log(`Already subscribed to topic: ${topic}`);
  }
}

function getLatestLiveMessage(topic) {
  return latestMessages[topic] || null;
}

function isTopicSubscribed(topic) {
  return subscribedTopics.has(topic);
}

function unsubscribeFromTopic(topic) {
  if (subscribedTopics.has(topic)) {
    device.unsubscribe(topic);
    subscribedTopics.delete(topic);
    console.log(`Unsubscribed from topic: ${topic}`);
  } else {
    console.log(`Not subscribed to topic: ${topic}`);
  }
}

module.exports = {
  subscribeToTopic,
  getLatestLiveMessage,
  isTopicSubscribed,
  unsubscribeFromTopic,
};

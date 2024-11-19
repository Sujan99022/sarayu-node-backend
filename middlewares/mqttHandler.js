const awsIot = require("aws-iot-device-sdk");
const MessageModel = require("../models/mqtt-message-model");

let latestMessages = {}; // Store the latest live message per topic
let subscribedTopics = new Set(); // Store subscribed topics

// AWS IoT Core device configuration
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
    const messageData = JSON.parse(payload.toString());
    const timestamp = new Date();

    // Save the latest message in-memory for quick access
    latestMessages[topic] = { message: messageData, timestamp };

    // Also save the message to MongoDB
    await MessageModel.findOneAndUpdate(
      { topic },
      { $push: { messages: { message: messageData, timestamp } } },
      { upsert: true, new: true }
    );
  } catch (error) {
    console.error("Error processing message:", error);
  }
});

// Function to subscribe to topics dynamically
const subscribeToTopic = (topic) => {
  device.subscribe(topic, (err) => {
    if (err) {
      console.error(`Failed to subscribe to topic ${topic}`, err);
    } else {
      console.log(`Subscribed to topic ${topic}`);
      subscribedTopics.add(topic); // Add to subscribed topics set
    }
  });
};

const unsubscribeFromTopic = (topic) => {
  device.unsubscribe(topic, (err) => {
    if (err) {
      console.error(`Failed to unsubscribe from topic ${topic}`, err);
    } else {
      console.log(`Unsubscribed from topic ${topic}`);
      subscribedTopics.delete(topic); // Remove from subscribed topics set
    }
  });
};

// Function to check if a topic is subscribed
const isTopicSubscribed = (topic) => {
  return subscribedTopics.has(topic); // Check if the topic exists in the set
};

// Function to get the latest live message from memory
const getLatestLiveMessage = (topic) => {
  return latestMessages[topic] || null;
};

module.exports = {
  subscribeToTopic,
  getLatestLiveMessage,
  isTopicSubscribed,
  unsubscribeFromTopic,
};

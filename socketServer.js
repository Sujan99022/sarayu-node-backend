const winston = require("winston");
const dotenv = require("dotenv");
const http = require("http");
const { Server } = require("socket.io");
const { subscribeToTopic, getLatestLiveMessage } = require("./middlewares/mqttHandler");
const SubscribedTopic = require("./models/subscribed-topic-model");
const express = require("express");
const connectDB = require("./env/db");

// Load environment variables
dotenv.config({ path: "./.env" });

// Connect to MongoDB
connectDB();

// Logger configuration
const logger = winston.createLogger({
  level: "info",
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  ),
  transports: [
    new winston.transports.File({ filename: "error.log", level: "error" }),
    new winston.transports.File({ filename: "combined.log" }),
  ],
});

// Initialize HTTP server and Socket.IO server
const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
  },
});

// Active topics and their intervals
const activeTopics = new Map();

// Socket.IO Logic
io.on("connection", (socket) => {
  const subscriptions = new Map();

  // Subscribe to a topic
  socket.on("subscribeToTopic", async (topic) => {
    if (!topic || subscriptions.has(topic)) return;

    try {
      socket.join(topic);
      subscriptions.set(topic, true);

      if (!activeTopics.has(topic)) {
        activeTopics.set(topic, { clients: new Set(), lastMessage: null, interval: null });
        startTopicStream(topic);
      }

      activeTopics.get(topic).clients.add(socket.id);

      const latestMessage = await getLatestLiveMessage(topic);
      if (latestMessage) {
        socket.emit("liveMessage", { success: true, message: latestMessage, topic });
      }
    } catch (error) {
      logger.error(`Subscription error for ${topic}: ${error.message}`);
    }
  });

  // Unsubscribe from a topic
  socket.on("unsubscribeFromTopic", (topic) => {
    if (subscriptions.has(topic)) {
      socket.leave(topic);
      subscriptions.delete(topic);

      if (activeTopics.has(topic)) {
        const topicData = activeTopics.get(topic);
        topicData.clients.delete(socket.id);

        if (topicData.clients.size === 0) {
          clearInterval(topicData.interval); // Clear interval when no clients are subscribed
          activeTopics.delete(topic);
        }
      }
    }
  });

  // Handle disconnection
  socket.on("disconnect", () => {
    subscriptions.forEach((_, topic) => {
      socket.leave(topic);

      if (activeTopics.has(topic)) {
        const topicData = activeTopics.get(topic);
        topicData.clients.delete(socket.id);

        if (topicData.clients.size === 0) {
          clearInterval(topicData.interval); // Clear interval when no clients are subscribed
          activeTopics.delete(topic);
        }
      }
    });
    subscriptions.clear();
  });
});

// Start streaming for topic
const startTopicStream = (topic) => {
  const topicData = activeTopics.get(topic);

  // Use a single interval per topic
  topicData.interval = setInterval(async () => {
    try {
      const latestMessage = await getLatestLiveMessage(topic);
      if (latestMessage && (!topicData.lastMessage || topicData.lastMessage.message.message !== latestMessage.message.message)) {
        io.to(topic).emit("liveMessage", { success: true, message: latestMessage, topic });
        topicData.lastMessage = latestMessage;
      }
    } catch (error) {
      logger.error(`Stream error for ${topic}: ${error.message}`);
    }
  }, 200); // Adjust interval as needed
};

// Start server after DB connection
const socketPort = process.env.SOCKET_PORT || 4000;
server.listen(socketPort, "0.0.0.0", () => {
  logger.info(`Socket.IO Server running on port ${socketPort}`);

  // Subscribe to topics after 5 seconds
  setTimeout(async () => {
    try {
      const SubscribedTopicList = await SubscribedTopic.find({}, { _id: 0, topic: 1 });
      if (SubscribedTopicList?.length > 0) {
        await Promise.all(SubscribedTopicList.map(({ topic }) => subscribeToTopic(topic)));
        logger.info("MQTT topics subscribed successfully");
      }
    } catch (err) {
      logger.error(`Error subscribing to topics: ${err.message}`);
    }
  }, 5000);
});
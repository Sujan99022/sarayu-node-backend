// mqttRoutes.js
const express = require("express");
const {
  getLatestLiveMessage,
  subscribeToTopic,
  isTopicSubscribed,
  unsubscribeFromTopic,
} = require("../middlewares/mqttHandler");
const MessageModel = require("../models/mqtt-message-model");
const AllTopicsModel = require("../models/all-mqtt-messages");
const moment = require("moment-timezone");

const router = express.Router();

// Endpoint to subscribe to a new topic
router.post("/subscribe", (req, res) => {
  const { topic } = req.body;
  if (!topic) {
    return res
      .status(400)
      .json({ success: false, message: "Topic is required" });
  }

  subscribeToTopic(topic);
  res.json({ success: true, message: `Subscribed to topic: ${topic}` });
});

// Route to fetch the latest live message for a specific topic
router.post("/messages", (req, res) => {
  const { topic } = req.body;
  if (!topic) {
    return res
      .status(400)
      .json({ success: false, message: "Topic is required" });
  }

  // Retrieve the latest live message from memory
  const latestMessage = getLatestLiveMessage(topic);
  if (!latestMessage) {
    return res
      .status(404)
      .json({ success: false, message: "No live message available" });
  }

  res.json({ success: true, message: latestMessage });
});

// Route to fetch messages from the last 2 hours
router.post("/realtime-data/last-2-hours", async (req, res) => {
  const { topic } = req.body;
  console.log(topic);
  if (!topic) {
    return res.status(400).json({ error: "Topic is required" });
  }

  try {
    const twoHoursAgo = moment()
      .tz("Asia/Kolkata")
      .subtract(2, "hours")
      .toDate();

    const messages = await MessageModel.findOne({
      topic,
      messages: { $elemMatch: { timestamp: { $gte: twoHoursAgo } } },
    })
      .select({
        topic: 1,
        messages: {
          $filter: {
            input: "$messages",
            as: "message",
            cond: { $gte: ["$$message.timestamp", twoHoursAgo] },
          },
        },
      })
      .sort({ "messages.timestamp": -1 });

    res.json(messages);
  } catch (error) {
    console.error("Error fetching data:", error);
    res.status(500).send("Internal Server Error");
  }
});

// POST /api/topics/add/:topic
router.post("/add", async (req, res) => {
  try {
    const { topic } = req.query;
    const { thresholds } = req.body;
    console.log(thresholds);
    if (!topic) {
      return res.status(400).json({ error: "Topic name is required" });
    }

    if (!Array.isArray(thresholds) || thresholds.length === 0) {
      return res
        .status(400)
        .json({ error: "Thresholds are required and must be an array" });
    }

    const existingTopic = await AllTopicsModel.findOne({ topic });

    if (existingTopic) {
      existingTopic.thresholds = thresholds;
      await existingTopic.save();

      return res.status(200).json({
        message: "Thresholds updated successfully",
        topic: existingTopic,
      });
    }

    const newTopic = new AllTopicsModel({ topic, thresholds });
    await newTopic.save();

    res.status(201).json({ topic: newTopic });
  } catch (error) {
    res
      .status(500)
      .json({ error: "Internal server error", details: error.message });
  }
});

// GET /api/topics/get/:topic
router.get("/get", async (req, res) => {
  try {
    const { topic } = req.query;

    if (!topic) {
      return res.status(400).json({ error: "Topic name is required" });
    }

    const topicData = await AllTopicsModel.findOne({ topic });
    if (!topicData) {
      return res.status(404).json({ error: "Topic not found" });
    }

    res.status(200).json({ data: topicData });
  } catch (error) {
    res
      .status(500)
      .json({ error: "Internal server error", details: error.message });
  }
});

// Route to check if a topic is subscribed
router.get("/is-subscribed", (req, res) => {
  const { topic } = req.query;

  if (!topic) {
    return res
      .status(400)
      .json({ success: false, message: "Topic is required" });
  }

  const isSubscribed = isTopicSubscribed(topic);
  res.json({ success: true, isSubscribed });
});

router.post("/unsubscribe", (req, res) => {
  const { topic } = req.body;

  if (!topic) {
    return res
      .status(400)
      .json({ success: false, message: "Topic is required" });
  }

  if (!isTopicSubscribed(topic)) {
    return res
      .status(400)
      .json({ success: false, message: "Topic is not subscribed" });
  }

  unsubscribeFromTopic(topic);

  res.json({ success: true, message: `Unsubscribed from topic: ${topic}` });
});

// Helper function to get the start and end of a day
const getDayRange = (date) => {
  const start = new Date(date.setHours(0, 0, 0, 0));
  const end = new Date(date.setHours(23, 59, 59, 999));
  return { start, end };
};

// Get today's highest value
router.get("/todays-highest", async (req, res) => {
  const { topic } = req.query;
  const { start, end } = getDayRange(new Date());

  try {
    const result = await MessageModel.aggregate([
      { $match: { topic } },
      { $unwind: "$messages" },
      {
        $match: {
          "messages.timestamp": { $gte: start, $lte: end },
        },
      },
      {
        $addFields: {
          "messages.message": { $toDouble: "$messages.message" },
        },
      },
      {
        $sort: { "messages.message": -1 },
      },
      { $limit: 1 },
    ]);

    res
      .status(200)
      .json(
        result.length ? result[0].messages : { message: "No data available" }
      );
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Get yesterday's highest value
router.get("/yesterdays-highest", async (req, res) => {
  const { topic } = req.query;
  const yesterday = new Date();
  yesterday.setDate(yesterday.getDate() - 1);
  const { start, end } = getDayRange(yesterday);

  try {
    const result = await MessageModel.aggregate([
      { $match: { topic } },
      { $unwind: "$messages" },
      {
        $match: {
          "messages.timestamp": { $gte: start, $lte: end },
        },
      },
      {
        $addFields: {
          "messages.message": { $toDouble: "$messages.message" },
        },
      },
      {
        $sort: { "messages.message": -1 },
      },
      { $limit: 1 },
    ]);

    res
      .status(200)
      .json(
        result.length ? result[0].messages : { message: "No data available" }
      );
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Get the highest value in the last 7 days
router.get("/last-7-days-highest", async (req, res) => {
  const { topic } = req.query;
  const last7Days = new Date();
  last7Days.setDate(last7Days.getDate() - 7);

  try {
    const result = await MessageModel.aggregate([
      { $match: { topic } },
      { $unwind: "$messages" },
      {
        $match: {
          "messages.timestamp": { $gte: last7Days },
        },
      },
      {
        $addFields: {
          "messages.message": { $toDouble: "$messages.message" },
        },
      },
      {
        $sort: { "messages.message": -1 },
      },
      { $limit: 1 },
    ]);

    res
      .status(200)
      .json(
        result.length ? result[0].messages : { message: "No data available" }
      );
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;

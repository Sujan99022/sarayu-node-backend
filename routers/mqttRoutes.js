// mqttRoutes.js
const express = require("express");
const {
  getLatestLiveMessage,
  subscribeToTopic,
  isTopicSubscribed,
  unsubscribeFromTopic,
  updateThresholds,
} = require("../middlewares/mqttHandler");
const MessageModel = require("../models/mqtt-message-model");
const AllTopicsModel = require("../models/all-mqtt-messages");
const moment = require("moment-timezone");
const SubscribedTopic = require("../models/subscribed-topic-model");

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

router.post("/create-tagname", async (req, res) => {
  try {
    const { topic } = req.body;
    const existingMessage = await MessageModel.findOne({ topic });
    if (existingMessage) {
      return res.status(400).json({
        success: false,
        message: "TagName already exists!",
      });
    }

    await MessageModel.create({ topic, messages: [] });
    res.status(201).json({
      success: true,
      data: [],
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: "Internal server error",
    });
  }
});

router.get("/get-all-subscribedtopics", async (req, res) => {
  try {
    const subscribedTopicList = await SubscribedTopic.find({}, { _id: 0, topic: 1 });
    const topics = subscribedTopicList.map(item => item.topic);
    res.status(200).json({ success: true, data: topics });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: "Internal server error",
    });
  }
});


router.get("/get-all-tagname", async (req, res) => {
  try {
    const topics = await MessageModel.aggregate([
      {
        $project: {
          topic: 1,
          isEmpty: { $eq: [{ $size: "$messages" }, 0] },
        },
      },
      { $sort: { _id: -1 } },
    ]);

    res.status(200).json({
      success: true,
      data: topics,
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: "Internal server error",
    });
  }
});

router.get("/get-recent-5-tagname", async (req, res) => {
  try {
    const topics = await MessageModel.find(
      { messages: { $size: 0 } },
      { topic: 1, _id: 0 }
    )
      .sort({ _id: -1 })
      .limit(5);

    res.status(200).json({
      success: true,
      data: topics,
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: "Internal server error",
    });
  }
});

router.delete("/delete-topic/:topic", async (req, res) => {
  try {
    const { topic } = req.params;
    const message = await MessageModel.findOne({ topic });
    if (!message) {
      return res
        .status(404)
        .json({ success: false, message: "No topic found" });
    }
    await message.deleteOne();
    res.status(200).json({ success: true, message: "Topic deleted" });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: "Internal server error",
    });
  }
});

router.post("/subscribe-to-all", async (req, res) => {
  try {
    const topics = await MessageModel.find({}, { topic: 1, _id: 0 });
    if (!topics.length) {
      return res.status(404).json({
        success: false,
        message: "No topics found to subscribe to.",
      });
    }

    topics.forEach((t) => {
      subscribeToTopic(t.topic);
    });

    res.status(200).json({
      success: true,
      message: "Subscribed to all topics successfully.",
      data: topics.map((t) => t.topic),
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: "Internal server error",
    });
  }
});

router.post("/unsubscribe-from-all", async (req, res) => {
  try {
    const topics = await MessageModel.find({}, { topic: 1, _id: 0 });

    if (!topics.length) {
      return res.status(404).json({
        success: false,
        message: "No topics found to unsubscribe from.",
      });
    }

    const unsubscribedTopics = [];

    topics.forEach((t) => {
      if (isTopicSubscribed(t.topic)) {
        unsubscribeFromTopic(t.topic);
        unsubscribedTopics.push(t.topic);
      }
    });

    if (!unsubscribedTopics.length) {
      return res.status(400).json({
        success: false,
        message: "No topics were subscribed.",
      });
    }

    res.status(200).json({
      success: true,
      message: "Unsubscribed from all subscribed topics successfully.",
      data: unsubscribedTopics,
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: "Internal server error",
    });
  }
});

// Route to fetch the latest live message for a specific topic
router.post("/messages", (req, res) => {
  const { topic } = req.body;
  if (!topic) {
    return res
      .status(400)
      .json({ success: false, message: "Topic is required" });
  }
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

router.post("/report-filter", async (req, res) => {
  const { topic, from, to, minValue } = req.body;
  console.log("check body:", req.body);

  // Validate the input
  if (!topic || !from || !to || minValue === undefined) {
    return res.status(400).json({
      error: "Topic, from date, to date, and minValue are required.",
    });
  }

  try {
    // Convert `from` and `to` dates to the correct timezone
    const fromDate = moment(from).tz("Asia/Kolkata").toDate();
    const toDate = moment(to).tz("Asia/Kolkata").toDate();

    // Query MongoDB
    const result = await MessageModel.aggregate([
      { $match: { topic } }, // Match the correct topic
      {
        $project: {
          topic: 1,
          messages: {
            $filter: {
              input: "$messages",
              as: "message",
              cond: {
                $and: [
                  { $gte: ["$$message.timestamp", fromDate] },
                  { $lte: ["$$message.timestamp", toDate] },
                  { $gte: ["$$message.message", Number(minValue)] },
                ],
              },
            },
          },
        },
      },
    ]);

    // Check if messages are found
    if (!result.length || result[0].messages.length === 0) {
      return res
        .status(404)
        .json({ error: "No messages found for the given criteria." });
    }

    // Sort the messages in descending order of timestamp
    const sortedMessages = result[0].messages.sort(
      (a, b) => new Date(b.timestamp) - new Date(a.timestamp)
    );

    // Respond with the filtered and sorted messages
    res.status(200).json({
      topic: result[0].topic,
      messages: sortedMessages,
    });
  } catch (error) {
    console.error("Error fetching data:", error);
    res.status(500).send("Internal Server Error");
  }
});

router.post("/realtime-data/custom-range", async (req, res) => {
  const { topic, from, to, granularity } = req.body;

  if (!topic || !from || !to || !granularity) {
    return res
      .status(400)
      .json({ error: "Topic, from, to, and granularity are required" });
  }

  try {
    const fromDate = moment.tz(from, "Asia/Kolkata").toDate();
    const toDate = moment.tz(to, "Asia/Kolkata").toDate();

    if (fromDate > toDate) {
      return res
        .status(400)
        .json({ error: "'From' date cannot be later than 'To' date" });
    }

    const data = await MessageModel.findOne({
      topic,
      messages: { $elemMatch: { timestamp: { $gte: fromDate, $lte: toDate } } },
    }).select({
      messages: {
        $filter: {
          input: "$messages",
          as: "message",
          cond: {
            $and: [
              { $gte: ["$$message.timestamp", fromDate] },
              { $lte: ["$$message.timestamp", toDate] },
            ],
          },
        },
      },
    });

    if (!data || !data.messages || data.messages.length === 0) {
      return res.json({ topic, messages: [] });
    }

    // Process data based on granularity
    const groupedMessages = processMessages(data.messages, granularity);

    res.json({ success: true, topic, messages: groupedMessages });
  } catch (error) {
    console.error("Error fetching custom range data:", error);
    res.status(500).send("Internal Server Error");
  }
});

// Helper function to group and average messages
function processMessages(messages, granularity) {
  const grouped = {};

  messages.forEach((msg) => {
    const timeKey = moment(msg.timestamp).startOf(granularity).toISOString();
    if (!grouped[timeKey]) {
      grouped[timeKey] = { count: 0, sum: 0 };
    }
    grouped[timeKey].count += 1;
    grouped[timeKey].sum += msg.message;
  });

  // Convert grouped data into the desired format
  return Object.entries(grouped).map(([time, { count, sum }]) => ({
    timestamp: time,
    message: sum / count,
  }));
}

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
      updateThresholds(topic,thresholds);
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

const mongoose = require("mongoose");

const thresholdSchema = new mongoose.Schema({
  value: {
    type: Number,
    required: true,
  },
  color: {
    type: String,
    required: true,
  },
});

const topicSchema = new mongoose.Schema({
  topic: {
    type: String,
    required: true,
    unique: true,
  },
  thresholds: {
    type: [thresholdSchema],
    default: [],
  },
});

// Create the Topic model
const AllTopicsModel = mongoose.model("Topic", topicSchema);

module.exports = AllTopicsModel;

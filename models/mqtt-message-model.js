// mqtt-message-model.js
const mongoose = require("mongoose");

const messageSchema = new mongoose.Schema({
  message: { type: Number, required: true }, // Store the numeric value of the message
  timestamp: { type: Date, index: true, default: Date.now }, // Timestamp of the message
});

const mqttMessageSchema = new mongoose.Schema({
  topic: { type: String, required: true, unique: true }, // Topic to identify the messages
  messages: [messageSchema], // Array of messages with the correct schema
});

module.exports = mongoose.model("MqttMessage", mqttMessageSchema);

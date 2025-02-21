// mqtt-message-model.js
const mongoose = require("mongoose");

const messageSchema = new mongoose.Schema({
  message: { type: Number, required: true, index: true }, // Index on message field
  timestamp: { type: Date, default: Date.now, index: true }, // Index on timestamp field
});

const mqttMessageSchema = new mongoose.Schema({
  topic: { type: String, required: true, unique: true, index: true }, // Index on topic field
  messages: [messageSchema], // Indexes inside subdocuments are not automatically created
});

// Create a compound index for better querying
mqttMessageSchema.index({ topic: 1, "messages.timestamp": 1 });

module.exports = mongoose.model("MqttMessage", mqttMessageSchema);

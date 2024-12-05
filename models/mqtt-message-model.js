// mqtt-message-model.js
const mongoose = require("mongoose");

const messageSchema = new mongoose.Schema({
  message: { type: Number, required: true },
  timestamp: { type: Date, index: true, default: Date.now },
});

const mqttMessageSchema = new mongoose.Schema({
  topic: { type: String, required: true, unique: true },
  messages: [messageSchema],
});

module.exports = mongoose.model("MqttMessage", mqttMessageSchema);

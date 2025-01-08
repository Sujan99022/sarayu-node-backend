const mongoose = require("mongoose");

const configSchema = new mongoose.Schema({
  gateway: {
    type: String,
    required: true,
  },
  slaveid: {
    type: String,
    required: true,
  },
  address: {
    type: String,
    required: true,
  },
  functioncode: {
    type: String,
    required: true,
  },
  size: {
    type: String,
    required: true,
  },
});

const ConfigDevice = mongoose.model("ConfigDevice", configSchema);

module.exports = ConfigDevice;

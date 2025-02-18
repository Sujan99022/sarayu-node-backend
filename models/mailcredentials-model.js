const mongoose = require("mongoose");

const mailCredentials = new mongoose.Schema({
  email: {
    type: String,
    required: [true, "Email is required"],
  },
  appPassword: {
    type: String,
    required: [true, "App password is required"],
  },
  active: {
    type: Boolean,
    default: false,
  },
});

const MailCred = mongoose.model("MailCred", mailCredentials);
module.exports = MailCred;

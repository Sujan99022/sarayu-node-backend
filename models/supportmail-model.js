const mongoose = require("mongoose");

const SupportMailSchema = new mongoose.Schema(
  {
    username: {
      type: String,
      required: [true, "Username name is required"],
    },
    email: {
      type: String,
      required: [true, "Email address is required"],
    },
    subject: {
      type: String,
      required: [true, "Subject is required"],
    },
    description: {
      type: String,
      required: [true, "Description is required"],
    },
    delete: {
      type: Boolean,
      default: false,
    },
  },
  {
    timestamps: true,
  }
);
const SupportMail = mongoose.model("SupportMail", SupportMailSchema);
module.exports = SupportMail;

const asyncHandler = require("../middlewares/asyncHandler");
const ErrorResponse = require("../middlewares/errorResponse");
const SupportMail = require("../models/supportmail-model");
const sendMail = require("../utils/mail");
const MailCred = require("../models/mailcredentials-model");

const handleMessage = asyncHandler(async (req, res, next) => {
  const { username, email, subject, description } = req.body;
  const newMessage = new SupportMail({
    username,
    email,
    subject,
    description,
  });
  await newMessage.save();
  req.io.emit("newMessage", newMessage);

  res.status(201).json({ success: true, data: newMessage });
});

const getMessages = asyncHandler(async (req, res, next) => {
  const messages = await SupportMail.find({ delete: false }).sort({
    createdAt: -1,
  });
  res.status(200).json(messages);
});

const sendMailtoCustomer = async (req, res, next) => {
  const { adminMail, appPassword, email, subject, text } = req.body;
  try {
    await sendMail(adminMail, appPassword, email, subject, text);
    res.status(200).json({
      success: true,
      message: "Email sent successfully",
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: "Failed to send email",
      error: error.message,
    });
  }
};

const deleteMessage = asyncHandler(async (req, res, next) => {
  const { id } = req.params;
  const message = await SupportMail.findById(id);
  if (!message) {
    return next(new ErrorResponse(`No mail found with id ${id}`, 404));
  }
  await message.deleteOne();
  res.status(200).json({ success: true, data: [] });
});

const softDeleteMessage = asyncHandler(async (req, res, next) => {
  const { id } = req.params;
  const message = await SupportMail.findById(id);
  if (!message) {
    return res
      .status(404)
      .json({ success: false, message: "Message not found" });
  }
  message.delete = true;
  await message.save();
  res.status(200).json({ success: true, data: [] });
});

const restoreSoftDeleteMessage = asyncHandler(async (req, res, next) => {
  const { id } = req.params;
  const message = await SupportMail.findById(id);
  message.delete = false;
  await message.save();
  res.status(200).json({
    success: true,
    data: [],
  });
});

const getAllSoftDeletedMessage = asyncHandler(async (req, res, next) => {
  const message = await SupportMail.find({ delete: true });
  res.status(200).json({
    success: true,
    data: message,
  });
});

const addMailCredentials = asyncHandler(async (req, res, next) => {
  const { email, appPassword } = req.body;
  const credMail = await MailCred.findOne({ email, appPassword });
  if (!credMail) {
    await MailCred.create({ email, appPassword });
    res.status(201).json({ success: true, data: [] });
  } else {
    return next(new ErrorResponse("Email, appPassword already exists!"));
  }
});

const deleteMailCredential = asyncHandler(async (req, res, next) => {
  const { id } = req.params;
  const mailCred = await MailCred.findById(id);
  if (mailCred.active) {
    return next(new ErrorResponse("Active cred can't be deleted!", 500));
  }
  await mailCred.deleteOne();
  res.status(200).json({ success: true, data: [] });
});

const setActiveMailCred = asyncHandler(async (req, res, next) => {
  const { id } = req.params;
  await MailCred.updateMany({}, { active: false });
  const mailCred = await MailCred.findById(id);
  if (!mailCred) {
    return res.status(404).json({
      success: false,
      message: "MailCred not found",
    });
  }
  mailCred.active = true;
  await mailCred.save();
  res.status(200).json({
    success: true,
    data: mailCred,
  });
});

const createMailCredAndSetActive = asyncHandler(async (req, res, next) => {
  const { email, appPassword } = req.body;
  await MailCred.updateMany({}, { active: false });
  await MailCred.create({ email, appPassword, active: true });
  res.status(200).json({
    success: true,
    data: [],
  });
});

const getMailCredentials = asyncHandler(async (req, res, next) => {
  const cred = await MailCred.find({ active: true });
  res.status(200).json({
    success: true,
    data: cred,
  });
});

const getAllMails = asyncHandler(async (req, res, next) => {
  const credMail = await MailCred.find({});
  res.status(200).json({
    success: true,
    data: credMail,
  });
});

module.exports = {
  handleMessage,
  getMessages,
  sendMailtoCustomer,
  deleteMessage,
  softDeleteMessage,
  getAllSoftDeletedMessage,
  restoreSoftDeleteMessage,
  addMailCredentials,
  getMailCredentials,
  setActiveMailCred,
  getAllMails,
  deleteMailCredential,
  createMailCredAndSetActive,
};

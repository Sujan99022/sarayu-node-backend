const express = require("express");
const {
  handleMessage,
  getMessages,
  deleteMessage,
  sendMailtoCustomer,
  softDeleteMessage,
  getAllSoftDeletedMessage,
  restoreSoftDeleteMessage,
  addMailCredentials,
  getMailCredentials,
  getAllMails,
  deleteMailCredential,
  setActiveMailCred,
  createMailCredAndSetActive,
} = require("../controllers/supportmail-controller");
const router = express.Router();

// Attach the Socket.IO instance to the request object
router.use((req, res, next) => {
  req.io = req.app.get("socketio");
  next();
});

router.post("/", handleMessage);
router.get("/", getMessages);
router.get("/getSoftDeletedMessage", getAllSoftDeletedMessage);
router.delete("/:id", deleteMessage);
router.delete("/deleteMailCred/:id", deleteMailCredential);
router.get("/allMailCred", getAllMails);
router.post("/setActiveMailCred/:id", setActiveMailCred);
router.post("/softDelete/:id", softDeleteMessage);
router.post("/softDelete/restore/:id", restoreSoftDeleteMessage);
router.post("/sendreply", sendMailtoCustomer);
router.post("/mailCred", addMailCredentials);
router.get("/mailCred", getMailCredentials);
router.post("/createMailCredSetActive", createMailCredAndSetActive);

module.exports = router;

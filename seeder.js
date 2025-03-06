const connectDB = require("./env/db");
const User = require("./models/user-model");
const SupportMail = require("./models/supportmail-model");
const Admin = require("./models/admin-model");
const dotenv = require("dotenv");
const fs = require("fs");
const MqttMessage = require('./models/mqtt-message-model')

dotenv.config({ path: "./env/config.env" });

// const user_data = JSON.parse(fs.readFileSync("./data/user-data.json", "utf-8"));
// const support_data = JSON.parse(
//   fs.readFileSync("./data/support-data.json", "utf-8")
// );
const admin_data = JSON.parse(
  fs.readFileSync("./data/admin-data.json", "utf-8")
);
// const topics_data = JSON.parse(
//   fs.readFileSync("./data/topics-data.json", "utf-8")
// );

connectDB();

const insertData = async () => {
  try {
    // await User.create(user_data);
    // await SupportMail.create(support_data);
    // await Admin.create(admin_data);
    await MqttMessage.create(topics_data)
    console.log("Data insertion successful!");
  } catch (error) {
    console.error("Error inserting data:", error.message);
  } finally {
    process.exit();
  }
};

const deleteData = async () => {
  try {
    // await User.deleteMany();
    console.log("Data destroyed!");
    // await SupportMail.deleteMany();
    await MqttMessage.deleteMany()
  } catch (error) {
    console.error("Error deleting data:", error.message);
  } finally {
    process.exit();
  }
};

if (process.argv[2] === "-i") {
  insertData();
}
if (process.argv[2] === "-d") {
  deleteData();
}

// Import necessary modules and dependencies
const winston = require("winston");
const connectDB = require("./env/db");
const express = require("express");
const morgan = require("morgan");
const cors = require("cors");
const cookieParser = require("cookie-parser");
const fileupload = require("express-fileupload");
const errorHandler = require("./middlewares/error");
const dotenv = require("dotenv");
const authRoute = require("./routers/auth-router");
const supportmailRoute = require("./routers/supportmail-router");
const http = require("http");
const socketIo = require("socket.io");
const mqttRoutes = require("./routers/mqttRoutes");
const {
  subscribeToTopic,
  getLatestLiveMessage,
} = require("./middlewares/mqttHandler");
const SubscribedTopic = require("./models/subscribed-topic-model");

// Load environment variables
dotenv.config({ path: "./.env" });

// Create an Express application
const app = express();
const server = http.createServer(app);
const io = socketIo(server);

// Winston logger setup
const logger = winston.createLogger({
  level: "info",
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  ),
  transports: [
    new winston.transports.File({ filename: "error.log", level: "error" }),
    new winston.transports.File({ filename: "combined.log" }),
  ],
});

if (process.env.NODE_ENV === "development") {
  logger.add(
    new winston.transports.Console({
      format: winston.format.simple(),
    })
  );
}

// Middleware setup
app.use(express.json());
app.use(fileupload());
app.use(express.urlencoded({ extended: false }));
app.use(
  cors({ origin: "*", methods: ["GET", "POST", "PUT", "DELETE", "PATCH"] })
);
app.use(cookieParser());
app.use(morgan("dev"));

// Log each request
app.use((req, res, next) => {
  logger.info(`Requested to: ${req.url}`, {
    method: req.method,
    body: req.body,
  });
  next();
});

// Socket.IO connection handler
io.on("connection", (socket) => {
  socket.on("subscribeToTopic", async (topic) => {
    if (!topic) {
      socket.emit("error", { success: false, message: "Topic is required" });
      return;
    }

    const intervalId = setInterval(async () => {
      try {
        const latestMessage = await getLatestLiveMessage(topic);
        if (!latestMessage) {
          socket.emit("noData", {
            success: false,
            message: "No live message available",
          });
        } else {
          socket.emit("liveMessage", { success: true, message: latestMessage });
        }
      } catch (err) {
        logger.error(`Error fetching latest live message: ${err.message}`);
      }
    }, 1000);

    socket.on("disconnect", () => clearInterval(intervalId));
    socket.on("unsubscribeFromTopic", () => clearInterval(intervalId));
  });
});

// Register routes
app.use("/api/v1/auth", authRoute);
app.use("/api/v1/supportmail", supportmailRoute);
app.use("/api/v1/mqtt", mqttRoutes);
app.set("socketio", io);

// Error handling middleware
app.use(errorHandler);

// Connect to the database
connectDB();

// Start the server
const port = process.env.PORT || 5000;
server.listen(port, "0.0.0.0", async () => {
  try {
    const SubscribedTopicList = await SubscribedTopic.find(
      {},
      { _id: 0, topic: 1 }
    );
    if (SubscribedTopicList && SubscribedTopicList.length > 0) {
      for (const item of SubscribedTopicList) {
        await subscribeToTopic(item.topic);
      }
    }
    logger.info(`Listening on port number ${port}`);
  } catch (err) {
    logger.error(`Error during server startup: ${err.message}`);
  }
});

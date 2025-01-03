const winston = require("winston"); // Import Winston for logging
const connectDB = require("./env/db"); // Import the database connection function
const express = require("express"); // Import the Express framework
const morgan = require("morgan"); // Import Morgan, a logging middleware
const cors = require("cors"); // Import CORS middleware to handle Cross-Origin Resource Sharing
const cookieParser = require("cookie-parser"); // Import middleware to parse cookies
const fileupload = require("express-fileupload"); // Import middleware to handle file uploads
const errorHandler = require("./middlewares/error");
const dotenv = require("dotenv"); // Import dotenv to load environment variables from a .env file
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

// Load environment variables from .env file
dotenv.config({ path: "./.env" });

const app = express(); // Create an Express application
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

// Add console logging in development environment
if (process.env.NODE_ENV === "development") {
  logger.add(
    new winston.transports.Console({
      format: winston.format.simple(),
    })
  );
}

// Middleware setup
app.use(express.json());
app.use(fileupload()); // Enable file uploads
app.use(express.urlencoded({ extended: false })); // Parse URL-encoded data

app.use(
  cors({
    origin: "*", // Allow all origins
    methods: ["GET", "POST", "PUT", "DELETE", "PATCH"], // Allow these HTTP methods
  })
);

app.use(cookieParser()); // Enable cookie parsing
app.use(morgan("dev"));

// Add logger for each request
app.use((req, res, next) => {
  logger.info(`Requested to: ${req.url}`, {
    method: req.method,
    body: req.body,
  });
  next();
});

// Socket.IO connection
io.on("connection", (socket) => {
  socket.on("subscribeToTopic", (topic) => {
    if (!topic) {
      socket.emit("error", { success: false, message: "Topic is required" });
      return;
    }

    const intervalId = setInterval(() => {
      const latestMessage = getLatestLiveMessage(topic);

      if (!latestMessage) {
        socket.emit("noData", {
          success: false,
          message: "No live message available",
        });
      } else {
        socket.emit("liveMessage", { success: true, message: latestMessage });
      }
    }, 1000);

    socket.on("disconnect", () => {
      clearInterval(intervalId);
    });

    socket.on("unsubscribeFromTopic", () => {
      clearInterval(intervalId);
    });
  });
});

// Routers
app.use("/api/v1/auth", authRoute);
app.use("/api/v1/supportmail", supportmailRoute);
app.use("/api/v1/mqtt", mqttRoutes);

app.set("socketio", io);

// Error handling middleware
app.use(errorHandler);

// Connect to the database
connectDB();

const port = process.env.PORT || 5000;

// Start the server on port 5000
server.listen(port, "0.0.0.0", async () => {
  const SubscribedTopicList = await SubscribedTopic.find(
    {},
    { _id: 0, topic: 1 }
  );
  if (SubscribedTopicList) {
    SubscribedTopicList.forEach((item) => {
      subscribeToTopic(item);
    });
  }
  logger.info(`Listening on port number ${port}`);
});

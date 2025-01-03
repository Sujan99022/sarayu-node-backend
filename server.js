// Import necessary modules and dependencies
const winston = require("winston"); // Winston for logging
const connectDB = require("./env/db"); // Database connection utility
const express = require("express"); // Express framework
const morgan = require("morgan"); // HTTP request logging middleware
const cors = require("cors"); // Middleware for handling Cross-Origin Resource Sharing
const cookieParser = require("cookie-parser"); // Middleware for parsing cookies
const fileupload = require("express-fileupload"); // Middleware for handling file uploads
const errorHandler = require("./middlewares/error"); // Error handling middleware
const dotenv = require("dotenv"); // Load environment variables from a .env file
const authRoute = require("./routers/auth-router"); // Auth routes
const supportmailRoute = require("./routers/supportmail-router"); // Support mail routes
const http = require("http"); // HTTP server
const socketIo = require("socket.io"); // Socket.io for real-time communication
const mqttRoutes = require("./routers/mqttRoutes"); // MQTT routes
const {
  subscribeToTopic, // Function to subscribe to MQTT topics
  getLatestLiveMessage, // Function to fetch the latest live message for a topic
} = require("./middlewares/mqttHandler");
const SubscribedTopic = require("./models/subscribed-topic-model"); // Model for subscribed topics

// Load environment variables from the .env file
dotenv.config({ path: "./.env" });

// Create an Express application
const app = express();
const server = http.createServer(app); // HTTP server instance
const io = socketIo(server); // Socket.io instance for real-time communication

// Winston logger setup
const logger = winston.createLogger({
  level: "info", // Default log level set to "info"
  format: winston.format.combine(
    winston.format.timestamp(), // Add timestamp to each log
    winston.format.json() // Log in JSON format for structured logging
  ),
  transports: [
    new winston.transports.File({ filename: "error.log", level: "error" }), // Log errors to error.log
    new winston.transports.File({ filename: "combined.log" }), // Log all levels to combined.log
  ],
});

// Add console logging in the development environment
if (process.env.NODE_ENV === "development") {
  logger.add(
    new winston.transports.Console({
      format: winston.format.simple(), // Simple log format for the console
    })
  );
}

// Middleware setup
app.use(express.json()); // Parse incoming JSON requests
app.use(fileupload()); // Enable file uploads
app.use(express.urlencoded({ extended: false })); // Parse URL-encoded data

// Configure CORS (Cross-Origin Resource Sharing)
app.use(
  cors({
    origin: "*", // Allow all origins (change for production)
    methods: ["GET", "POST", "PUT", "DELETE", "PATCH"], // Allow these HTTP methods
  })
);

app.use(cookieParser()); // Enable cookie parsing
app.use(morgan("dev")); // Log HTTP requests using Morgan

// Custom logger middleware to log each request
app.use((req, res, next) => {
  logger.info(`Requested to: ${req.url}`, {
    method: req.method, // Log the HTTP method (GET, POST, etc.)
    body: req.body, // Log the request body
  });
  next(); // Call the next middleware in the stack
});

// Socket.IO connection handler
io.on("connection", (socket) => {
  // Handle subscription to a topic
  socket.on("subscribeToTopic", (topic) => {
    if (!topic) {
      socket.emit("error", { success: false, message: "Topic is required" });
      return; // Exit if the topic is missing
    }

    // Fetch the latest live message for the subscribed topic every second
    const intervalId = setInterval(() => {
      const latestMessage = getLatestLiveMessage(topic);

      // Handle no live message available
      if (!latestMessage) {
        socket.emit("noData", {
          success: false,
          message: "No live message available",
        });
      } else {
        // Emit live message to the client
        socket.emit("liveMessage", { success: true, message: latestMessage });
      }
    }, 1000);

    // Clean up the interval when the socket disconnects or unsubscribes
    socket.on("disconnect", () => {
      clearInterval(intervalId);
    });

    socket.on("unsubscribeFromTopic", () => {
      clearInterval(intervalId);
    });
  });
});

// Register API routes
app.use("/api/v1/auth", authRoute); // Authentication routes
app.use("/api/v1/supportmail", supportmailRoute); // Support mail routes
app.use("/api/v1/mqtt", mqttRoutes); // MQTT routes

app.set("socketio", io); // Make Socket.IO available globally in the app

// Error handling middleware (placed after routes)
app.use(errorHandler);

// Connect to the database
connectDB();

// Set the port from environment variables or default to 5000
const port = process.env.PORT || 5000;

// Start the server and listen on the specified port
server.listen(port, "0.0.0.0", async () => {
  // Fetch all subscribed topics from the database
  const SubscribedTopicList = await SubscribedTopic.find(
    {},
    { _id: 0, topic: 1 }
  );

  if (SubscribedTopicList) {
    // Subscribe to each topic found in the database
    SubscribedTopicList.forEach((item) => {
      subscribeToTopic(item);
    });
  }

  // Log that the server is listening on the specified port
  logger.info(`Listening on port number ${port}`);
});

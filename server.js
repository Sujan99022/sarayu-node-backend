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
const mqttRoutes = require("./routers/mqttRoutes");

// Load environment variables
dotenv.config({ path: "./.env" });

// Initialize Express
const app = express();

// Logger configuration
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

// Middleware
app.use(express.json());
app.use(fileupload());
app.use(express.urlencoded({ extended: false }));
app.use(cors({ origin: "*", methods: ["GET", "POST", "PUT", "DELETE", "PATCH"] }));
app.use(cookieParser());

// Logging
app.use((req, res, next) => {
  logger.info(`Requested to: ${req.url}`, {
    method: req.method,
    body: req.body,
  });
  next();
});

// Routes
app.use("/api/v1/auth", authRoute);
app.use("/api/v1/supportmail", supportmailRoute);
app.use("/api/v1/mqtt", mqttRoutes);

// Error handling
app.use(errorHandler);

// Database connection
connectDB();

// Start server
const port = process.env.PORT || 5000;
app.listen(port, "0.0.0.0", () => {
  logger.info(`API Server running on port ${port}`);
});
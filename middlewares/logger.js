// Importing the Winston library for logging
const winston = require("winston");

// Create a logger instance with specific settings
const logger = winston.createLogger({
  level: "info", // Set the default log level to "info" (logs info and higher levels)

  // Combine multiple formats for log output
  format: winston.format.combine(
    winston.format.timestamp(), // Add a timestamp to each log entry
    winston.format.json() // Format the log entry in JSON format (good for structured logs)
  ),

  // Define where to send the logs
  transports: [
    // Log errors to the "error.log" file
    new winston.transports.File({ filename: "error.log", level: "error" }),

    // Log all levels (info, warn, error) to the "combined.log" file
    new winston.transports.File({ filename: "combined.log" }),
  ],
});

// If the environment is development, add console logging for easier debugging
if (process.env.NODE_ENV === "development") {
  logger.add(
    new winston.transports.Console({
      // Use a simple format for console logs (no JSON formatting)
      format: winston.format.simple(),
    })
  );
}

// Export the logger instance to use in other parts of the application
module.exports = logger;

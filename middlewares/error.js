// Importing the custom error response and logger utilities
const ErrorResponse = require("./errorResponse");
const logger = require("./logger");

const errorHandler = (err, req, res, next) => {
  // Create a copy of the error object and assign the message
  let error = { ...err };
  error.message = err.message;

  // Log the error to the logger (error.log file)
  logger.error(`Error occurred: ${error.message}`, {
    stack: err.stack || "No stack available", // Stack trace if available
    statusCode: error.statusCode || 500, // Status code or default to 500 (Internal Server Error)
    url: req.originalUrl, // The URL where the error occurred
    method: req.method, // The HTTP method (GET, POST, etc.)
    body: req.body, // The body of the request for context
    timestamp: new Date().toISOString(), // Timestamp of when the error occurred
  });

  // Handle specific error types

  // Handle "CastError" (e.g., invalid MongoDB ObjectId)
  if (err.name === "CastError") {
    const message = `Resource not found with id of ${err.value}`;
    error = new ErrorResponse(message, 404); // Create a new ErrorResponse with a 404 status code
  }

  // Handle "11000" MongoDB duplicate key error code (duplicate field)
  if (err.code === 11000) {
    const message = "Duplicate field value entered";
    error = new ErrorResponse(message, 400); // Create a new ErrorResponse with a 400 status code
  }

  // Handle "ValidationError" (e.g., mongoose validation errors)
  if (err.name === "ValidationError") {
    const message = Object.values(err.errors) // Collect validation error messages
      .map((val) => val.message) // Map through errors to get the message
      .join(", "); // Join all error messages into a single string
    error = new ErrorResponse(message, 400); // Create a new ErrorResponse with a 400 status code
  }

  // Send the error response to the client with the appropriate status and message
  res.status(error.statusCode || 500).json({
    success: false, // Indicate the request was unsuccessful
    error: error.message || "Server Error", // Return the error message or default to "Server Error"
  });
};

module.exports = errorHandler; // Export the error handler for use in other parts of the application

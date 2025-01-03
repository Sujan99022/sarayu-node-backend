const ErrorResponse = require("./errorResponse");
const logger = require("./logger");

const errorHandler = (err, req, res, next) => {
  let error = { ...err };
  error.message = err.message;

  // Log the error to error.log
  logger.error(`Error occurred: ${error.message}`, {
    stack: err.stack || "No stack available",
    statusCode: error.statusCode || 500,
    url: req.originalUrl,
    method: req.method,
    body: req.body,
    timestamp: new Date().toISOString(),
  });

  // Handle specific error types
  if (err.name === "CastError") {
    const message = `Resource not found with id of ${err.value}`;
    error = new ErrorResponse(message, 404);
  }

  if (err.code === 11000) {
    const message = "Duplicate field value entered";
    error = new ErrorResponse(message, 400);
  }

  if (err.name === "ValidationError") {
    const message = Object.values(err.errors)
      .map((val) => val.message)
      .join(", ");
    error = new ErrorResponse(message, 400);
  }

  res.status(error.statusCode || 500).json({
    success: false,
    error: error.message || "Server Error",
  });
};

module.exports = errorHandler;

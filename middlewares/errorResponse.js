// Define a custom error class to handle application-specific error responses
class ErrorResponse extends Error {
  // Constructor takes a message and statusCode as parameters
  constructor(message, statusCode) {
    // Call the parent class constructor (Error) with the message
    super(message);

    // Set the custom status code for the error
    this.statusCode = statusCode;

    // Capture the stack trace (helpful for debugging)
    Error.captureStackTrace(this, this.constructor);
  }
}

// Export the custom error class for use in other files
module.exports = ErrorResponse;

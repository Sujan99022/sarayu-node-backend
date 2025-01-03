// Async handler wrapper to handle asynchronous functions in Express routes
// It ensures that any async errors are passed to the error handling middleware

const asyncHandler = (fn) => (req, res, next) =>
  // Return a Promise that resolves the async function and catches any errors
  Promise.resolve(fn(req, res, next)).catch(next);

// Export the asyncHandler function for use in other files
module.exports = asyncHandler;

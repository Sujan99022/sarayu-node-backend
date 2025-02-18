const axios = require("axios");

async function verifyEmailExistence(email) {
  const HUNTER_API_KEY = "your_hunter_api_key";

  try {
    const response = await axios.get(
      // `https://api.hunter.io/v2/email-verifier`,
      {
        params: { email, api_key: HUNTER_API_KEY },
      }
    );

    const { result, status } = response.data.data;
    console.log("Email verification result:", result, status);

    return result === "deliverable";
  } catch (error) {
    console.error("Error verifying email with Hunter API:", error.message);
    return false;
  }
}

module.exports = { verifyEmailExistence };

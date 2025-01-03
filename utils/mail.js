// Import nodemailer to send emails
const nodemailer = require("nodemailer");

// Function to send an email using Gmail service
const sendMail = async (email, subject, text) => {
  try {
    // Create a transporter object using Gmail service for email sending
    const transporter = nodemailer.createTransport({
      service: "gmail", // Use Gmail's email service
      auth: {
        user: "sujanrumakantha@gmail.com", // Your Gmail address (should be environment-secure)
        pass: "rblbgyipbnvblote", // Your app-specific password (should be environment-secure)
      },
    });

    // Define email options
    const mailOptions = {
      from: "sujanrumakantha@gmail.com", // Sender email address
      to: email, // Recipient email address
      subject: subject, // Email subject
      text: text, // Email body text
    };

    // Send the email using the defined transporter and options
    await transporter.sendMail(mailOptions);
    console.log("Email sent successfully");
  } catch (error) {
    // Catch any errors that occur while sending the email and log them
    console.error("Error sending email:", error);
    throw error; // Re-throw error so that it can be handled by calling function
  }
};

// Export the sendMail function for use in other modules
module.exports = sendMail;

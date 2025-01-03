const nodemailer = require("nodemailer");

const sendMail = async (email, subject, text) => {
  try {
    const transporter = nodemailer.createTransport({
      service: "gmail",
      auth: {
        user: "",
        pass: "",
      },
    });
    const mailOptions = {
      from: "",
      to: email,
      subject,
      text,
    };
    await transporter.sendMail(mailOptions);
  } catch (error) {
    res.status({});
    throw error;
  }
};

module.exports = sendMail;

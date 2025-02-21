/* This code snippet is a JavaScript function that connects to a MongoDB database using Mongoose, which
is an Object Data Modeling (ODM) library for MongoDB and Node.js. Here's a breakdown of what the
code does: */
const mongoose = require("mongoose");
// mongodb://localhost:27017/sarayu-project-local-db
//ec2 : mongodb://65.1.185.30:27017/sarayu-database-ec2
// "mongodb+srv://samithrgowda:7zsJuGajQ7ONZicL@srdbcluster.b8lex.mongodb.net/SRDB2?retryWrites=true&w=majority&appName=SRDBCLUSTER"
const connectDB = () => {
  mongoose
    .connect("mongodb://localhost:27017/sarayu-project-local-db")
    .then(() => {
      console.log("Database connection successfull!");
    })
    .catch((error) => {
      console.log("Database connection failed!", error);
    });
};

module.exports = connectDB;

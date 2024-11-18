/* This code snippet is a JavaScript function that connects to a MongoDB database using Mongoose, which
is an Object Data Modeling (ODM) library for MongoDB and Node.js. Here's a breakdown of what the
code does: */
const mongoose = require("mongoose");
// mongodb://localhost:27017/sarayu-project-local-db
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

// `mongodb+srv://sujanr:${encodeURIComponent(
//   "Sujanr@2001"
// )}@cluster0.iuybdds.mongodb.net/sarayuDatabase?retryWrites=true&w=majority&appName=Cluster0`

//localDB : mongodb://localhost:27017/SRDB
//cloud : mongodb+srv://samithrgowda:7zsJuGajQ7ONZicL@srdbcluster.b8lex.mongodb.net/?retryWrites=true&w=majority&appName=SRDBCLUSTER

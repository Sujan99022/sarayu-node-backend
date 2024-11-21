/* This JavaScript code is setting up a Node.js server using Express framework. Here's a breakdown of
what each part of the code is doing: */
const connectDB = require("./env/db"); // Import the database connection function
const express = require("express"); // Import the Express framework
const morgan = require("morgan"); // Import Morgan, a logging middleware
const cors = require("cors"); // Import CORS middleware to handle Cross-Origin Resource Sharing
const cookieParser = require("cookie-parser"); // Import middleware to parse cookies
const fileupload = require("express-fileupload"); // Import middleware to handle file uploads
const errorHandler = require("./middlewares/error");
const dotenv = require("dotenv"); // Import dotenv to load environment variables from a .env file
const authRoute = require("./routers/auth-router");
const supportmailRoute = require("./routers/supportmail-router");
const http = require("http");
const socketIo = require("socket.io");
const mqttRoutes = require("./routers/mqttRoutes");
const { subscribeToTopic } = require("./middlewares/mqttHandler");

// Load environment variables from .env file
dotenv.config({ path: "./.env" });

const app = express(); // Create an Express application
const server = http.createServer(app);
const io = socketIo(server);

// Middleware setup
app.use(express.json());
app.use(fileupload()); // Enable file uploads
app.use(express.urlencoded({ extended: false })); // Parse URL-encoded data

app.use(
  cors({
    // origin: ["http://localhost:3000"], // Allow only this origin
    origin: "*", // Allow all the origin
    methods: ["GET", "POST", "PUT", "DELETE", "PATCH"], // Allow these HTTP methods
  })
);

app.use(cookieParser()); // Enable cookie parsing
app.use(morgan("dev"));

// Socket.IO connection
io.on("connection", (socket) => {
  console.log("New client connected");
  socket.on("disconnect", () => {
    console.log("Client disconnected");
  });
});

//Routers
app.use("/api/v1/auth", authRoute);
app.use("/api/v1/supportmail", supportmailRoute);
app.use("/api/v1/mqtt", mqttRoutes);

app.set("socketio", io);
// Error handling middleware
app.use(errorHandler);

// Connect to the database
connectDB();

const port = process.env.PORT || 5000;

// Start the server on port 5000
server.listen(port, "0.0.0.0", () => {
  console.log(`Listening on port number ${port}`);
});

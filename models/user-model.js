const mongoose = require("mongoose");
const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");

// Define the user schema
const userSchema = new mongoose.Schema(
  {
    name: {
      type: String,
      required: [true, "Name is required"],
    },
    email: {
      type: String,
      required: [true, "Email is required"],
      unique: true,
      match: [/.+\@.+\..+/, "Please enter a valid email address"],
    },
    password: {
      type: String,
      select: false,
      required: [true, "Password is required"],
    },
    role: {
      type: String,
      enum: {
        values: ["manager", "supervisor", "employee"],
        message: "Role must be either manager, supervisor, or employee",
      },
      default: "employee",
    },
  },
  {
    timestamps: true,
  }
);

// Pre-save middleware to hash the password before saving to database
userSchema.pre("save", async function (next) {
  if (!this.isModified("password")) {
    return next();
  }
  const salt = await bcrypt.genSalt(10);
  this.password = await bcrypt.hash(this.password, salt);
  next();
});

userSchema.methods.getToken = function () {
  return jwt.sign(
    { id: this._id, name: this.name, email: this.email, role: this.role },
    process.env.JWT_SECRET,
    {
      expiresIn: "3d",
    }
  );
};

//method to verify the user entered password with the existing password in the database
userSchema.methods.verifyPass = async function (enteredPassword) {
  return await bcrypt.compare(enteredPassword, this.password);
};

// Create the user model from the schema
const User = mongoose.model("User", userSchema);

// Export the user model
module.exports = User;

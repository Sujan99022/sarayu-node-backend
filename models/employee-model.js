const mongoose = require("mongoose");
const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");

const employeeSchema = new mongoose.Schema(
  {
    name: {
      type: String,
      required: true,
    },
    email: {
      type: String,
      required: true,
      unique: true,
    },
    phonenumber: {
      type: String,
      required: false,
    },
    company: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "Company",
      required: true,
    },
    // room: {
    //   type: mongoose.Schema.Types.ObjectId,
    //   ref: "Room",
    //   required: true,
    // },
    supervisor: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "Supervisor",
      required: false,
    },
    password: {
      type: String,
      select: false,
      required: [true, "Password is required"],
    },
    topics: {
      type: [String],
      default: [
        "sarayu/device1/increment",
        "sarayu/device1/decrement",
        "sarayu/device1/random",
        "sarayu/device1/random1",
        "sarayu/device1/random2",
        "sarayu/device1/water",
      ],
    },
    favorites: {
      type: [String],
      default: [],
    },
    role: {
      type: String,
      default: "employee",
    },
  },
  {
    timestamps: true,
  }
);

// Pre-save middleware to hash the password before saving to database
employeeSchema.pre("save", async function (next) {
  if (!this.isModified("password")) {
    return next();
  }
  const salt = await bcrypt.genSalt(10);
  this.password = await bcrypt.hash(this.password, salt);
  next();
});

// method to generate the jwt token for the loggedin or signedup users
employeeSchema.methods.getToken = function () {
  return jwt.sign(
    {
      id: this._id,
      name: this.name,
      email: this.email,
      role: this.role,
      topic: this.mqttTopic,
    },
    process.env.JWT_SECRET,
    {
      expiresIn: "3d",
    }
  );
};

//method to verify the user entered password with the existing password in the database
employeeSchema.methods.verifyPass = async function (enteredPassword) {
  return await bcrypt.compare(enteredPassword, this.password);
};

const Employee = mongoose.model("Employee", employeeSchema);

module.exports = Employee;

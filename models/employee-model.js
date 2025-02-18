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
    },
    favorites: {
      type: [String],
      default: [],
    },
    graphwl: {
      type: [String],
      default: [],
    },
    layout: {
      type: String,
      default: "layout1",
    },
    assignedDigitalMeters: {
      type: [
        {
          topic: String,
          meterType: String,
          minValue: Number,
          maxValue: Number,
          ticks: Number,
          label : String
        },
      ],
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

employeeSchema.pre("save", async function (next) {
  if (!this.isModified("password")) {
    return next();
  }
  const salt = await bcrypt.genSalt(10);
  this.password = await bcrypt.hash(this.password, salt);
  next();
});

employeeSchema.methods.getToken = function () {
  return jwt.sign(
    {
      id: this._id,
      name: this.name,
      email: this.email,
      role: this.role,
      assignedDigitalMeters: this.assignedDigitalMeters,
    },
    process.env.JWT_SECRET,
    {
      expiresIn: "3d",
    }
  );
};

employeeSchema.methods.verifyPass = async function (enteredPassword) {
  return await bcrypt.compare(enteredPassword, this.password);
};

const Employee = mongoose.model("Employee", employeeSchema);

module.exports = Employee;

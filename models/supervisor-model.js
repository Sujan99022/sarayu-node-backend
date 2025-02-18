const mongoose = require("mongoose");
const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");

const supervisorSchema = new mongoose.Schema(
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
    password: {
      type: String,
      select: false,
      required: [true, "Password is required"],
    },
    company: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "Company",
      required: true,
    },
    manager: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "Manager",
      required: false,
    },
    topics: {
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
    favorites: {
      type: [String],
      default: [],
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
      default: "supervisor",
    },
  },
  {
    timestamps: true,
  }
);

// Pre-save middleware to hash the password before saving to database
supervisorSchema.pre("save", async function (next) {
  if (!this.isModified("password")) {
    return next();
  }
  const salt = await bcrypt.genSalt();
  this.password = await bcrypt.hash(this.password, salt);
  next();
});

// Method to generate the jwt token for the logged-in or signed-up users
supervisorSchema.methods.getToken = function () {
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

// Method to verify the user-entered password with the existing password in the database
supervisorSchema.methods.verifyPass = async function (enteredPassword) {
  return await bcrypt.compare(enteredPassword, this.password);
};

// Add a virtual field to reverse populate employees under the supervisor
supervisorSchema.virtual("employees", {
  ref: "Employee", // The model to populate
  localField: "_id", // The field in Supervisor
  foreignField: "supervisor", // The field in Employee that references Supervisor
  justOne: false, // To get an array of employees
});

// Ensure virtual fields are included in the output
supervisorSchema.set("toObject", { virtuals: true });
supervisorSchema.set("toJSON", { virtuals: true });

const Supervisor = mongoose.model("Supervisor", supervisorSchema);

module.exports = Supervisor;

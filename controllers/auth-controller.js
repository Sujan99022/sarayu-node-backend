const asyncHandler = require("../middlewares/asyncHandler");
const ErrorResponse = require("../middlewares/errorResponse");
const User = require("../models/user-model");
const Manager = require("../models/manager-model");
const Company = require("../models/company-model");
const Room = require("../models/room-model");
const Employee = require("../models/employee-model");
const Admin = require("../models/admin-model");
const sendMail = require("../utils/mail");
const MailCred = require("../models/mailcredentials-model");
const mongoose = require("mongoose");
const bcrypt = require("bcryptjs");
const { subscribeToDevice } = require("../middlewares/mqttHandler");
const Supervisor = require("../models/supervisor-model");

const login = asyncHandler(async (req, res, next) => {
  const { email, password } = req.body;
  const user = await User.findOne({ email }).select("+password");
  if (!user) {
    return next(new ErrorResponse("Invalid Credentials", 401));
  }
  const isMatch = await user.verifyPass(password);
  if (!isMatch) {
    return next(new ErrorResponse("Invalid Credentials", 401));
  }
  const token = await user.getToken();
  res.status(200).json({
    success: true,
    token,
  });
});

const adminLogin = asyncHandler(async (req, res, next) => {
  const { email, password } = req.body;
  const user = await Admin.findOne({ email }).select("+password");
  if (!user) {
    return next(new ErrorResponse("Invalid Credentials", 401));
  }
  const isMatch = await user.verifyPass(password);
  if (!isMatch) {
    return next(new ErrorResponse("Invalid Credentials", 401));
  }
  const token = await user.getToken();
  res.status(200).json({
    success: true,
    data: user,
    token,
  });
});
const createCompany = asyncHandler(async (req, res, next) => {
  const { name, email, phonenumber, address } = req.body;
  const company = await Company.findOne({ name });
  if (company) {
    return next(new ErrorResponse("Company already exists!", 409));
  }

  const newCompany = new Company({ name, email, phonenumber, address });
  await newCompany.save();
  res.status(201).json({
    success: true,
    data: newCompany,
  });
});

//get single company
const getSingleCompany = asyncHandler(async (req, res, next) => {
  const { companyId } = req.params;
  const company = await Company.findById(companyId);
  if (!company) {
    return next(
      new ErrorResponse(`No company found with id ${companyId}`, 404)
    );
  }
  res.status(200).json({
    success: true,
    data: company,
  });
});

//delete company
const deleteCompany = asyncHandler(async (req, res, next) => {
  const { id } = req.params;
  const company = await Company.findById(id);
  if (!company) {
    return next(new ErrorResponse(`No company found with id ${id}`, 404));
  }
  await company.deleteOne();
  res.status(200).json({
    success: true,
    data: [],
  });
});

// Get all companies
const getAllCompanies = asyncHandler(async (req, res, next) => {
  const companies = await Company.find().sort({ createdAt: -1 });
  res.status(200).json(companies);
});

//get manger for a specific company
const companyManager = asyncHandler(async (req, res, next) => {
  const { companyId } = req.params;
  const manager = await Manager.findOne({ company: companyId }).populate(
    "company"
  );
  res.status(200).json({
    success: true,
    data: manager,
  });
});

//delete manager
const deleteManager = asyncHandler(async (req, res, next) => {
  const { id } = req.params;
  const manager = await Manager.findById(id);
  if (!manager) {
    return nect(new ErrorResponse(`No manager found with id ${id}`, 404));
  }
  await manager.deleteOne();
  res.status(200).json({
    success: true,
    data: [],
  });
});

// Manager login
const loginAsManager = asyncHandler(async (req, res, next) => {
  const { email, password } = req.body;
  const user = await Manager.findOne({ email })
    .select("+password")
    .populate("company");
  if (!user) {
    return next(new ErrorResponse("Invalid Credentials", 401));
  }
  const isMatch = await user.verifyPass(password);
  if (!isMatch) {
    return next(new ErrorResponse("Invalid Credentials", 401));
  }
  const token = await user.getToken();
  res.status(200).json({
    success: true,
    user,
    token,
  });
});

const getSinlgeManager = asyncHandler(async (req, res, next) => {
  const { id } = req.params;
  const manager = await Manager.findById(id).populate("company");
  if (!manager) {
    return next(new ErrorResponse(`No manager found with id ${id}`, 404));
  }

  res.status(200).json({
    success: true,
    data: manager,
  });
});

const getSinlgeEmployee = asyncHandler(async (req, res, next) => {
  const { id } = req.params;
  const employee = await Employee.findById(id)
    .populate("company")
    .populate("supervisor");
  if (!employee) {
    return next(new ErrorResponse(`No employee found with id ${id}`, 404));
  }
  res.status(200).json({
    success: true,
    data: employee,
  });
});

const createManager = asyncHandler(async (req, res, next) => {
  const { companyId } = req.params;
  const { name, email, password, phonenumber } = req.body;
  // const findManager = await Manager.findOne({ company: companyId });
  // if (findManager) {
  //   return next(new ErrorResponse("A manager already exists!", 409));
  // }
  const findMail = await Manager.findOne({ email });
  if (findMail) {
    return next(new ErrorResponse("Email already exists!", 400));
  }
  // const mailCred = await MailCred.findOne({ active: true });
  // await sendMail(
  //   mailCred.email,
  //   mailCred.appPassword,
  //   email,
  //   "Manager Login Credentails",
  //   `Email : ${email}, Password : ${password}`
  // );
  const manager = await Manager.create({
    name,
    email,
    password,
    phonenumber,
    company: companyId,
  });
  res.status(201).json({
    success: true,
    data: manager,
  });
});

//get all manager of a company
const getAllManager = asyncHandler(async (req, res, next) => {
  const { companyId } = req.params;
  const managers = await Manager.find({ company: companyId }).populate(
    "company"
  );
  res.status(200).json({
    success: true,
    data: managers,
  });
});

//create room
const createRoom = asyncHandler(async (req, res, next) => {
  const { companyId } = req.params;
  const { name } = req.body;
  let room = await Room.create({ name, company: companyId });
  room = await Room.findById(room._id).populate("company");
  res.status(201).json({ success: true, data: room });
});

const getRooms = asyncHandler(async (req, res, next) => {
  const { companyId } = req.params;
  const rooms = await Room.find({ company: companyId }).populate("company");
  if (!rooms.length) {
    return res
      .status(404)
      .json({ success: false, message: "No rooms found for this company" });
  }
  res.status(200).json({ success: true, count: rooms.length, data: rooms });
});

const createSupervisor = asyncHandler(async (req, res, next) => {
  const { companyId } = req.params;
  const { name, email, password, phonenumber, mqttTopic } = req.body;
  console.log(password);
  const findSupervisor = await Supervisor.findOne({ email });
  if (findSupervisor) {
    return next(new ErrorResponse("Email already exists!", 500));
  }
  const supervisor = await Supervisor.create({
    name,
    email,
    password,
    phonenumber,
    mqttTopic,
    company: companyId,
  });
  res.status(201).json({
    success: true,
    data: supervisor,
  });
});

const createSupervisorAndAssignManager = asyncHandler(
  async (req, res, next) => {
    const { companyId, managerId } = req.params;
    const { name, email, password, phonenumber } = req.body;
    const findSupervisor = await Supervisor.findOne({ email });
    if (findSupervisor) {
      return next(new ErrorResponse("Email already exist!", 500));
    }
    const supervisor = await Supervisor.create({
      name,
      email,
      password,
      phonenumber,
      company: companyId,
      manager: managerId,
    });
    res.status(201).json({
      success: true,
      data: supervisor,
    });
  }
);

const getAllSupervisorOfSameCompany = asyncHandler(async (req, res, next) => {
  const { companyId } = req.params;

  const supervisors = await Supervisor.find({ company: companyId })
    .populate("company")
    .populate("manager")
    .populate("employees");

  res.status(200).json({
    success: true,
    count: supervisors.length,
    data: supervisors,
  });
});

//Login as employee
const loginAsEmployee = asyncHandler(async (req, res, next) => {
  const { email, password } = req.body;
  const user = await Employee.findOne({ email })
    .select("+password")
    .populate("company")
    .populate("supervisor");
  if (!user) {
    return next(new ErrorResponse("Invalid Credentials", 401));
  }
  const isMatch = await user.verifyPass(password);
  if (!isMatch) {
    return next(new ErrorResponse("Invalid Credentials", 401));
  }
  const token = await user.getToken();
  res.status(200).json({
    success: true,
    user,
    token,
  });
});

//create a employee
const createEmployee = asyncHandler(async (req, res, next) => {
  const { companyId, supervisorId } = req.params;
  const { name, email, password, phonenumber, mqttTopic } = req.body;
  const employee = await Employee.create({
    name,
    email,
    password,
    phonenumber,
    mqttTopic,
    company: companyId,
    supervisor: supervisorId,
  });
  res.status(201).json({
    success: true,
    data: employee,
  });
});

const createEmployeeWithoutSupervisor = asyncHandler(async (req, res, next) => {
  const { companyId } = req.params;
  const { name, email, password, phonenumber, mqttTopic } = req.body;
  const employee = await Employee.create({
    name,
    email,
    password,
    phonenumber,
    mqttTopic,
    company: companyId,
  });
  res.status(201).json({
    success: true,
    data: employee,
  });
});
//change supervisor of a company
const changeSupervisorForEmployee = asyncHandler(async (req, res, next) => {
  const { empId, supervisorId } = req.params;
  const employee = await Employee.findByIdAndUpdate(
    empId,
    { supervisor: supervisorId },
    { new: true }
  );
  res.status(200).json({
    success: true,
    data: employee,
  });
});

const changeManagerForSupervisor = asyncHandler(async (req, res, next) => {
  const { supervisorId, managerId } = req.params;
  const supervisor = await Supervisor.findByIdAndUpdate(
    supervisorId,
    { manager: managerId },
    { new: true }
  );
  res.status(200).json({
    success: true,
    data: supervisor,
  });
});

const getSinlgeSupervisor = asyncHandler(async (req, res, next) => {
  const { id } = req.params;
  const supervisor = await Supervisor.findById(id)
    .populate("company")
    .populate("manager");
  if (!supervisor) {
    return next(new ErrorResponse(`No supervisor found with id ${id}`, 404));
  }
  res.status(200).json({
    success: true,
    data: supervisor,
  });
});

const changeSupervisorForAllEmployee = asyncHandler(async (req, res, next) => {
  const { oldSupervisorId, newSupervisorId } = req.params;
  await Employee.updateMany(
    { supervisor: oldSupervisorId },
    { supervisor: newSupervisorId }
  );
  res.status(200).json({
    success: true,
    data: [],
  });
});

const swapSupervisorForAllEmployee = asyncHandler(async (req, res, next) => {
  const { firstSupervisorId, secondSupervisorId } = req.params;
  const temporarySupervisorId = new mongoose.Types.ObjectId();
  await Employee.updateMany(
    { supervisor: firstSupervisorId },
    { supervisor: temporarySupervisorId }
  );
  await Employee.updateMany(
    { supervisor: secondSupervisorId },
    { supervisor: firstSupervisorId }
  );
  await Employee.updateMany(
    { supervisor: temporarySupervisorId },
    { supervisor: secondSupervisorId }
  );
  res.status(200).json({
    success: true,
    message: "Supervisors swapped successfully for all employees.",
  });
});

const getAllEmployeesOfSameCompany = asyncHandler(async (req, res, next) => {
  const { companyId } = req.params;
  const employees = await Employee.find({ company: companyId })
    .populate("company")
    .populate("supervisor");

  res.status(200).json({ success: true, data: employees });
});

const removeSupervisorFromEmployee = asyncHandler(async (req, res, next) => {
  const { id } = req.params;
  const employee = await Employee.findByIdAndUpdate(
    id,
    { $unset: { supervisor: "" } },
    { new: true }
  );
  if (!employee) {
    return next(new ErrorResponse(`No employee found with id ${id}`, 404));
  }
  res.status(200).json({
    success: true,
    data: employee,
  });
});

const removeManagerFromSupervisor = asyncHandler(async (req, res, next) => {
  const { id } = req.params;
  const supervisor = await Supervisor.findByIdAndUpdate(
    id,
    { $unset: { manager: "" } },
    { new: true }
  );
  if (!supervisor) {
    return next(new ErrorResponse(`No supervisor found with id ${id}`, 404));
  }
  res.status(200).json({
    success: true,
    data: supervisor,
  });
});

const deleteAnyEmployeeCompany = asyncHandler(async (req, res, next) => {
  const { id } = req.params;
  const manager = await Manager.findById(id);
  const supervisor = await Supervisor.findById(id);
  const employee = await Employee.findById(id);

  if (manager) {
    await manager.deleteOne();
    return res.status(200).json({
      success: true,
      data: [],
    });
  }
  if (supervisor) {
    await Employee.updateMany(
      { supervisor: supervisor.id },
      { $unset: { supervisor: "" } }
    );
    await supervisor.deleteOne();
    return res.status(200).json({
      success: true,
      data: [],
    });
  }
  if (employee) {
    await employee.deleteOne();
    return res.status(200).json({
      success: true,
      data: [],
    });
  }
  return next(new ErrorResponse(`No user found with id ${id}`, 404));
});

const getAllOperatorsForSupervisor = asyncHandler(async (req, res, next) => {
  const { id } = req.params;
  const supervisor = await Supervisor.findById(id);
  if (!supervisor) {
    return next(
      new ErrorResponse(`No supervisor found with id ${supervisor}`, 404)
    );
  }
  const operators = await Employee.find({ supervisor: id });
  res.status(200).json({
    success: true,
    data: operators,
  });
});

// Supervisor login
const loginAsSupervisor = asyncHandler(async (req, res, next) => {
  const { email, password } = req.body;
  const user = await Supervisor.findOne({ email })
    .select("+password")
    .populate("company")
    .populate("employees");

  if (!user) {
    return next(new ErrorResponse("Invalid Credentials", 401));
  }

  const isMatch = await user.verifyPass(password);
  if (!isMatch) {
    return next(new ErrorResponse("Invalid Credentials", 401));
  }

  // Subscribe this user to their specific MQTT topic
  if (user.mqttTopic) {
    await subscribeToDevice(user, user.mqttTopic);
  }

  const token = await user.getToken();
  res.status(200).json({
    success: true,
    user,
    token,
  });
});

const resetPasswordForSupervisor = asyncHandler(async (req, res, next) => {
  const { email, activePassword, newPassword } = req.body;
  const supervisor = await Supervisor.findOne({ email }).select("+password");
  if (!supervisor) {
    return next(new ErrorResponse(`No user found with email ${email}`, 404));
  }
  const verifyPass = await supervisor.verifyPass(activePassword);
  if (!verifyPass) {
    return next(new ErrorResponse(`Active password did't matched`, 401));
  }
  supervisor.password = newPassword;
  await supervisor.save();
  res.status(200).json({
    success: true,
    data: "password changed successfully",
  });
});

const resetPasswordForEmployee = asyncHandler(async (req, res, next) => {
  const { email, newPassword, activePassword } = req.body;
  const employee = await Employee.findOne({ email }).select("+password");
  if (!employee) {
    return next(
      new ErrorResponse(`No employee found with email ${email}`, 404)
    );
  }
  const verifyPass = await employee.verifyPass(activePassword);
  if (!verifyPass) {
    return next(new ErrorResponse(`Active password did't matched`, 401));
  }
  employee.password = newPassword;
  await employee.save();
  res.status(200).json({
    success: true,
    data: "password changed successfully",
  });
});

const resetPasswordForManager = asyncHandler(async (req, res, next) => {
  const { email, newPassword, activePassword } = req.body;
  const manager = await Manager.findOne({ email }).select("+password");
  if (!manager) {
    return next(new ErrorResponse(`No manager found with email ${email}`, 404));
  }
  const verifyPass = await manager.verifyPass(activePassword);
  if (!verifyPass) {
    return next(new ErrorResponse(`Active password did't matched `, 401));
  }
  manager.password = newPassword;
  await manager.save();
  res.status(200).json({
    success: true,
    data: "password reseted successfully",
  });
});

const subscribeToEmployeeTopic = asyncHandler(async (req, res, next) => {
  await subscribeToDevice(req.body, req.body.mqttTopic);
  res.status(200).json({
    success: true,
    data: [],
  });
});

const addFavoriteSupervisor = async (req, res) => {
  try {
    const { id } = req.params;
    const { topic } = req.body;

    if (!topic) {
      return res.status(400).json({ message: "Topic is required" });
    }

    const supervisor = await Supervisor.findById(id);
    if (!supervisor) {
      return res.status(404).json({ message: "Supervisor not found" });
    }

    if (!supervisor.favorites.includes(topic)) {
      supervisor.favorites.push(topic);
      await supervisor.save();
    }

    res.status(200).json({
      message: "Topic added to favorites",
      favorites: supervisor.favorites,
    });
  } catch (error) {
    res
      .status(500)
      .json({ message: "Error adding favorite", error: error.message });
  }
};

// Remove a topic from favorites
const removeFavoriteSupervisor = async (req, res) => {
  try {
    const { id } = req.params;
    const { topic } = req.body;

    if (!topic) {
      return res.status(400).json({ message: "Topic is required" });
    }

    const supervisor = await Supervisor.findById(id);
    if (!supervisor) {
      return res.status(404).json({ message: "Supervisor not found" });
    }

    const index = supervisor.favorites.indexOf(topic);
    if (index !== -1) {
      supervisor.favorites.splice(index, 1);
      await supervisor.save();
    }

    res.status(200).json({
      message: "Topic removed from favorites",
      favorites: supervisor.favorites,
    });
  } catch (error) {
    res
      .status(500)
      .json({ message: "Error removing favorite", error: error.message });
  }
};
const addFavoriteEmployee = async (req, res) => {
  try {
    const { id } = req.params;
    const { topic } = req.body;

    if (!topic) {
      return res.status(400).json({ message: "Topic is required" });
    }

    const supervisor = await Employee.findById(id);
    if (!supervisor) {
      return res.status(404).json({ message: "Supervisor not found" });
    }

    if (!supervisor.favorites.includes(topic)) {
      supervisor.favorites.push(topic);
      await supervisor.save();
    }

    res.status(200).json({
      message: "Topic added to favorites",
      favorites: supervisor.favorites,
    });
  } catch (error) {
    res
      .status(500)
      .json({ message: "Error adding favorite", error: error.message });
  }
};

// Remove a topic from favorites
const removeFavoriteEmployee = async (req, res) => {
  try {
    const { id } = req.params;
    const { topic } = req.body;

    if (!topic) {
      return res.status(400).json({ message: "Topic is required" });
    }

    const supervisor = await Employee.findById(id);
    if (!supervisor) {
      return res.status(404).json({ message: "Supervisor not found" });
    }

    const index = supervisor.favorites.indexOf(topic);
    if (index !== -1) {
      supervisor.favorites.splice(index, 1);
      await supervisor.save();
    }

    res.status(200).json({
      message: "Topic removed from favorites",
      favorites: supervisor.favorites,
    });
  } catch (error) {
    res
      .status(500)
      .json({ message: "Error removing favorite", error: error.message });
  }
};

module.exports = {
  login,
  createCompany,
  deleteCompany,
  deleteAnyEmployeeCompany,
  getSingleCompany,
  getAllCompanies,
  companyManager,
  deleteManager,
  loginAsManager,
  getSinlgeManager,
  getAllManager,
  createManager,
  createRoom,
  getRooms,
  loginAsSupervisor,
  createSupervisor,
  getAllSupervisorOfSameCompany,
  loginAsEmployee,
  createEmployee,
  adminLogin,
  getAllEmployeesOfSameCompany,
  changeSupervisorForEmployee,
  changeSupervisorForAllEmployee,
  swapSupervisorForAllEmployee,
  getSinlgeSupervisor,
  createEmployeeWithoutSupervisor,
  removeSupervisorFromEmployee,
  createSupervisorAndAssignManager,
  changeManagerForSupervisor,
  removeManagerFromSupervisor,
  getSinlgeEmployee,
  getAllOperatorsForSupervisor,
  resetPasswordForSupervisor,
  resetPasswordForEmployee,
  resetPasswordForManager,
  subscribeToEmployeeTopic,
  addFavoriteSupervisor,
  removeFavoriteSupervisor,
  addFavoriteEmployee,
  removeFavoriteEmployee,
};

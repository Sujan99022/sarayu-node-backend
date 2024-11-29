const express = require("express");
const {
  login,
  createCompany,
  deleteCompany,
  deleteAnyEmployeeCompany,
  getAllCompanies,
  getSingleCompany,
  companyManager,
  loginAsManager,
  getSinlgeManager,
  getAllManager,
  deleteManager,
  createManager,
  createRoom,
  getRooms,
  loginAsSupervisor,
  createSupervisor,
  createEmployeeWithoutSupervisor,
  getSinlgeSupervisor,
  getAllSupervisorOfSameCompany,
  loginAsEmployee,
  createEmployee,
  changeSupervisorForEmployee,
  adminLogin,
  getAllEmployeesOfSameCompany,
  changeSupervisorForAllEmployee,
  swapSupervisorForAllEmployee,
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
} = require("../controllers/auth-controller");
const router = express.Router();

router.route("/login").post(login);
router.route("/companies").post(createCompany).get(getAllCompanies);
router.route("/company/:companyId").get(getSingleCompany);
router.route("/companies/:id").delete(deleteCompany);
router.route("/deleteAnyEmployee/:id").delete(deleteAnyEmployeeCompany);
router.route("/admin/login").post(adminLogin);
router.route("/manager/:companyId").get(companyManager);
router.route("/getallmanager/:companyId").get(getAllManager);
router.route("/manager/:id").delete(deleteManager);
router.route("/manager/login").post(loginAsManager);
router.route("/manager/:id").post(getSinlgeManager);
router.route("/manager/create/:companyId").post(createManager);
router.route("/room/:companyId").post(createRoom).get(getRooms);
router.route("/supervisor/create/:companyId").post(createSupervisor);
router
  .route("/supervisor/create/:companyId/:managerId")
  .post(createSupervisorAndAssignManager);
router.route("/supervisor/login").post(loginAsSupervisor);
router
  .route("/supervisor/getAllSupervisorOfSameCompany/:companyId")
  .get(getAllSupervisorOfSameCompany);
router.route("/supervisor/:id").get(getSinlgeSupervisor);
router.route("/employee/login").post(loginAsEmployee);
router.route("/employee/:id").get(getSinlgeEmployee);
router.route("/employee/create/:companyId/:supervisorId").post(createEmployee);
router
  .route("/employee/create/:companyId")
  .post(createEmployeeWithoutSupervisor);
router
  .route("/employee/changeSupervisor/:empId/:supervisorId")
  .post(changeSupervisorForEmployee);
router
  .route("/employee/changeManager/:supervisorId/:managerId")
  .post(changeManagerForSupervisor);
router
  .route(
    "/employee/changeSupervisorforAllEmployees/:oldSupervisorId/:newSupervisorId"
  )
  .post(changeSupervisorForAllEmployee);
router
  .route(
    "/employee/swaoSupervisorForAllEmployees/:firstSupervisorId/:secondSupervisorId"
  )
  .post(swapSupervisorForAllEmployee);
router
  .route("/employee/getAllEmployeesOfSameCompany/:companyId")
  .get(getAllEmployeesOfSameCompany);
router
  .route("/employee/removeSupervisor/:id")
  .post(removeSupervisorFromEmployee);
router.route("/supervisor/removeManager/:id").post(removeManagerFromSupervisor);
router
  .route("/supervisor/getalloperators/:id")
  .get(getAllOperatorsForSupervisor);
router.post("/manager/reset-password", resetPasswordForManager);
router.post("/supervisor/reset-password", resetPasswordForSupervisor);
router.post("/employee/reset-password", resetPasswordForEmployee);
router.post("/subscribeToEmployeeTopic", subscribeToEmployeeTopic);

router.post("/supervisor/:id/favorites", addFavoriteSupervisor);
router.delete("/supervisor/:id/favorites", removeFavoriteSupervisor);
router.post("/employee/:id/favorites", addFavoriteEmployee);
router.delete("/employee/:id/favorites", removeFavoriteEmployee);

module.exports = router;

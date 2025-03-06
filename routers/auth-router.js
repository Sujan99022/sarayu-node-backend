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
  addTagnamesToTheEmployee,
  deleteTagnamesToTheEmployee,
  addTagnamesToTheSupervisor,
  deleteTagnamesToTheSupervisor,
  addTagnamesToTheManager,
  deleteTagnamesToTheManager,
  assignDigitalMeterToEmployee,
  assignDigitalMeterToSupervisor,
  assignDigitalMeterToManager,
  subscribedTopics,
  getSubscribedTopics,
  addDeviceConfig,
  removeDeviceConfig,
  updateDeviceConfig,
  getAllDeviceConfig,
  assignlayoutToEmployee,
  assignlayoutToSupervisor,
  assignlayoutToManager,
  addGraphWatchListEmployee,
  removeGraphWatchListEmployee,
  addGraphWatchListSupervisor,
  removeGraphWatchListSupervisor,
  addFavoriteManager,
  removeFavoriteManager,
  addGraphWatchListManager,
  removeGraphWatchListManager
} = require("../controllers/auth-controller");
const router = express.Router();

router.route("/login").post(login);
router.route("/companies").post(createCompany).get(getAllCompanies);
router.route("/company/:companyId").get(getSingleCompany);
router.route("/companies/:id").delete(deleteCompany);
router.route("/deleteAnyEmployee/:id").delete(deleteAnyEmployeeCompany);
router.route("/admin/login").post(adminLogin);
// router.route("/manager/:companyId").get(companyManager);
router.route("/getallmanager/:companyId").get(getAllManager);
router.route("/manager/:id").delete(deleteManager);

router.route("/manager/:id/favorites").post(addFavoriteManager);
router.route("/manager/:id/favorites").delete(removeFavoriteManager);

router.route("/manager/login").post(loginAsManager);
router.route("/manager/:id").get(getSinlgeManager);
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

router.post("/employee/:id/graphwl", addGraphWatchListEmployee);
router.delete("/employee/:id/graphwl", removeGraphWatchListEmployee);

router.post("/supervisor/:id/graphwl", addGraphWatchListSupervisor);
router.delete("/supervisor/:id/graphwl", removeGraphWatchListSupervisor);

router.post("/manager/:id/graphwl", addGraphWatchListManager);
router.delete("/manager/:id/graphwl", removeGraphWatchListManager);


router.post("/employee/assign-topics/:id", addTagnamesToTheEmployee);
router.put("/employee/delete-topic/:id", deleteTagnamesToTheEmployee);
router.post("/supervisor/assign-topics/:id", addTagnamesToTheSupervisor);
router.put("/supervisor/delete-topic/:id", deleteTagnamesToTheSupervisor);
router.post("/manager/assign-topics/:id", addTagnamesToTheManager);
router.put("/manager/delete-topic/:id", deleteTagnamesToTheManager);
router
  .route("/subscribedTopics")
  .post(subscribedTopics)
  .get(getSubscribedTopics);

// device config route starts gere
router.route("/deviceconfig").get(getAllDeviceConfig).post(addDeviceConfig);
router
  .route("/deviceconfig/:id")
  .put(updateDeviceConfig)
  .delete(removeDeviceConfig);
// device config route ends gere

// assign layout to user logic starts here
router.route(`/layoutassign/employee/:id`).put(assignlayoutToEmployee);
router.route(`/layoutassign/supervisor/:id`).put(assignlayoutToSupervisor);
router.route(`/layoutassign/manager/:id`).put(assignlayoutToManager);
// assign layout to user logic ends here

// digital meter routes starts here
router.put("/digitalmeter/employee/:id", assignDigitalMeterToEmployee);
router.put("/digitalmeter/supervisor/:id", assignDigitalMeterToSupervisor);
router.put("/digitalmeter/manager/:id", assignDigitalMeterToManager);

// sample way to send body
// {
//   "assignedDigitalMeters": [
//     {
//       "topic": "meter1",
//       "meterType" : "Type 1",
//       "minValue": 0,
//       "maxValue": 100,
//       "ticks": [0, 5, 10, ..., 95, 100]
//     },
//     {
//       "topic": "newMeter",
//       "meterType" : "Type 2",
//       "minValue": 0,
//       "maxValue": 200,
//       "ticks": [0, 50, 100, ..., 175, 200]
//     }
//   ]
// }

// digital meter routes ends here

module.exports = router;

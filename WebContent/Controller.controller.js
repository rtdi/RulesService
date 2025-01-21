sap.ui.define([ "sap/ui/core/mvc/Controller"], function(Controller) {
	"use strict";

	return Controller.extend("io.rtdi.bigdata.rulesservice.Controller", {

		onInit : function() {
			var oModel = new sap.ui.model.json.JSONModel();
			oModel.loadData("./rest/config/service");
			this.getView().setModel(oModel);
		}

	});

});

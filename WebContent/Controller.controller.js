sap.ui.define([ "sap/ui/core/mvc/Controller"], function(Controller) {
	"use strict";

	return Controller.extend("io.rtdi.bigdata.rulesservice.Controller", {

		onInit : function() {
			var oModel = new sap.ui.model.json.JSONModel();
			var statusbox = this.getView().byId("systemstatus");
			oModel.attachRequestCompleted(function(event) {
				if (event.getParameter("success")) {
					statusbox.setVisible(true);
				}
			});
			oModel.attachRequestFailed(function(event) {
				var text = event.getParameter("responseText");
				sap.m.MessageToast.show("Reading status info failed: " + text);
			});
			this.getView().setModel(oModel);
			oModel.loadData("./rest/config/service");
		},
		onRefresh : function(event) {
			var model = this.getView().getModel();
			model.loadData("./rest/config/service");
		}

	});

});

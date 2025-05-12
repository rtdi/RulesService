sap.ui.define(["sap/ui/core/mvc/Controller"],
function(Controller) {"use strict";
return Controller.extend("io.rtdi.bigdata.rulesservice.ui5.Config", {
	onInit : function() {
		var model = new sap.ui.model.json.JSONModel();
		var statusbox = this.getView().byId("systemstatus");
		model.attachRequestCompleted(function(event) {
			if (event.getParameter("success")) {
				statusbox.setVisible(true);
			}
		});
		model.attachRequestFailed(function(event) {
			var text = event.getParameter("responseText");
			sap.m.MessageToast.show("Reading config failed: " + text);
		});
		this.getView().setModel(model);
		model.loadData("../rest/config");
	},
	onSave : function(event) {
		var model = this.getView().getModel();
		var post = new sap.ui.model.json.JSONModel();
		post.attachRequestFailed(function(event) {
			var text = event.getParameter("responseText");
			sap.m.MessageToast.show("Save failed: " + text);
		});
		post.attachRequestCompleted(function(event) {
			console.log(post.getProperty("/"));
			if (event.getParameter("success")) {
				sap.m.MessageToast.show("Saved");
				model.loadData("../rest/config");
			}
		});
		var json = JSON.stringify(model.getProperty("/"));
		var headers = {
			"Content-Type": "application/json;charset=utf-8"
		}
		post.loadData("../rest/config", json, true, "POST", false, true, headers);
	},
	onRefresh : function(event) {
		var model = this.getView().getModel();
		model.loadData("../rest/config");
	}
});
});


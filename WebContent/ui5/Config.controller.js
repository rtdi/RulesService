sap.ui.define(["sap/ui/core/mvc/Controller"],
function(Controller) {"use strict";
return Controller.extend("io.rtdi.bigdata.rulesservice.Config", {
	onInit : function() {
		var model = new sap.ui.model.json.JSONModel();
		model.attachRequestFailed(function(event) {
			var text = event.getParameter("responseText");
			sap.m.MessageToast.show("Reading config failed: " + text);
		});
		model.loadData("../rest/config");
		this.getView().setModel(model);
	},
	onSave : function(event) {
		var model = this.getView().getModel();
		var post = new sap.ui.model.json.JSONModel();
		post.attachRequestFailed(function(event) {
			var text = event.getParameter("responseText");
			sap.m.MessageToast.show("Save failed: " + text);
		});
		post.attachRequestCompleted(function() {
			console.log(post.getProperty("/"));
		});
		var json = JSON.stringify(model.getProperty("/"));
		var headers = {
			"Content-Type": "application/json;charset=utf-8"
		}
		post.loadData("../rest/config", json, true, "POST", false, true, headers);
	},
});
});


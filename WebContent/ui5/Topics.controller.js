sap.ui.define(["sap/ui/core/mvc/Controller"],
function(Controller) {"use strict";
return Controller.extend("io.rtdi.bigdata.rulesservice.ui5.Topics", {
	onInit : function() {
		var model = new sap.ui.model.json.JSONModel();
		this.getView().setModel(model);
		model.attachRequestFailed(function(event) {
			var text = event.getParameter("responseText");
			sap.m.MessageToast.show("Reading topic rules failed: " + text);
		});
		model.loadData("../rest/topicrules");
		var model2 = new sap.ui.model.json.JSONModel();
		this.getView().setModel(model2, "rulegroups");
		model2.attachRequestFailed(function(event) {
			var text = event.getParameter("responseText");
			sap.m.MessageToast.show("Reading rules failed: " + text);
		});
		model2.loadData("../rest/rules");
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
			}
		});
		var json = JSON.stringify(model.getProperty("/"));
		var headers = {
			"Content-Type": "application/json;charset=utf-8"
		}
		post.loadData("../rest/topicrules", json, true, "POST", false, true, headers);
	},
	onChange: function(event) {
		var model = this.getView().getModel();
		var context = event.getSource().getBindingContext();
		var path = context.getPath();
		model.setProperty(path + "/modified", true);
	},
	onRefresh : function(event) {
		var model = this.getView().getModel();
		model.loadData("../rest/topicrules");
		var model2 = this.getView().getModel("rulegroups");
		model2.loadData("../rest/rules");
	}

});
});


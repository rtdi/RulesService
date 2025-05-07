sap.ui.define(["sap/ui/core/mvc/Controller"],
function(Controller) {"use strict";
return Controller.extend("io.rtdi.bigdata.rulesservice.ui5.Status", {
	onInit : function() {
		var oModel = new sap.ui.model.json.JSONModel();
		oModel.loadData("../rest/config/service");
		this.getView().setModel(oModel);
	},
	onRefresh : function(event) {
		var model = this.getView().getModel();
		model.loadData("../rest/config/service");
	},
	onStartStop : function(event) {
		var model = this.getView().getModel();
		var source = event.getSource();
		var path = source.getBindingContext().getPath();
		var servicename = model.getProperty(path + "/topicname"); // path.substring(path.lastIndexOf("/") + 1);
		var status = model.getProperty(path + "/status"); // path.substring(path.lastIndexOf("/") - 1, path.lastIndexOf("/"));
		var newstatustext = status === true ? "stopping" : "starting"; 
		var post = new sap.ui.model.json.JSONModel();
		post.attachRequestFailed(function(event) {
			var text = event.getParameter("responseText");
			sap.m.MessageToast.show(newstatustext + " failed: " + text);
		});
		post.attachRequestCompleted(function(event) {
			console.log(post.getProperty("/"));
			if (event.getParameter("success")) {
				sap.m.MessageToast.show(newstatustext + " completed");
			}
			model.loadData("../rest/config/service");
		});
		var json = JSON.stringify(model.getProperty("/"));
		var headers = {
			"Content-Type": "application/json;charset=utf-8"
		}
		if (status === true) {
			post.loadData("../rest/topics/stop/" + servicename, json, true, "POST", false, true, headers);
        } else {
			post.loadData("../rest/topics/start/" + servicename, json, true, "POST", false, true, headers);
        }
	}
});
});


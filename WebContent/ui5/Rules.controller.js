sap.ui.define(["sap/ui/core/mvc/Controller"],
function(Controller) {"use strict";
return Controller.extend("io.rtdi.bigdata.rulesservice.Rules", {
	onInit : function() {
		var subjectsmodel = new sap.ui.model.json.JSONModel();
		subjectsmodel.attachRequestFailed(function(event) {
			var text = event.getParameter("responseText");
			sap.m.MessageToast.show("Reading subjects failed: " + text);
		});
		this.getView().setModel(subjectsmodel);
		subjectsmodel.loadData("../rest/subjects");
		var rulesmodel = new sap.ui.model.json.JSONModel();
		rulesmodel.attachRequestCompleted(function() {
			this.setProperty("/" + this.getProperty("/").length, {"name": "create new", "new": true})
		});
		rulesmodel.attachRequestFailed(function(event) {
			var text = event.getParameter("responseText");
			sap.m.MessageToast.show("Reading rules failed: " + text);
		});
		this.getView().setModel(rulesmodel, "rules");
	},
	onSubjectSelected : function(event) {
		var listitem = event.getSource();
		var context = listitem.getBindingContext();
		var record = context.getObject();
		var subjectname = record.name;
		var rulesmodel = this.getView().getModel("rules");
		var ruleslist = this.getView().byId("rules");
		ruleslist.data("subjectname", subjectname);
		rulesmodel.loadData("../rest/subjects/" + encodeURI(subjectname) + "/rules");
	},
	onRuleSelected : function(event) {
		var listitem = event.getSource();
		var context = listitem.getBindingContext("rules");
		var record = context.getObject();
		var rulename = record.name;
		var ruleslist = this.getView().byId("rules");
		var subjectname = ruleslist.data("subjectname");
		var url = window.location.href;
		var rulelink;
		if (record.new) {
			rulelink = "&new=true";
		} else {
			rulelink = "&rule=" + encodeURI(rulename);
		}
		
		var ruleurl = url.replace("Rules.html", "Rule.html?subject=" + encodeURI(subjectname) + rulelink);
		window.open(ruleurl, "_blank");
	}
});
});


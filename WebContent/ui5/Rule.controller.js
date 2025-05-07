sap.ui.define(["sap/ui/core/mvc/Controller"],
function(Controller) {"use strict";
return Controller.extend("io.rtdi.bigdata.rulesservice.ui5.Rule", {
	onInit : function() {
		var model = new sap.ui.model.json.JSONModel();
		var subjectname = jQuery.sap.getUriParameters().get("subject");
		var rulename = jQuery.sap.getUriParameters().get("rule");
		var isnew = jQuery.sap.getUriParameters().get("new");
		this.getView().setModel(model);
		model.attachRequestFailed(function(event) {
			var text = event.getParameter("responseText");
			sap.m.MessageToast.show("Reading rule file failed: " + text);
		});
		if (isnew) {
			model.loadData("../rest/subjects/" + encodeURI(subjectname) + "/defaultrule");
		} else {
			model.loadData("../rest/subjects/" + encodeURI(subjectname) + "/rules/" + rulename);
		}
		var topicmodel = new sap.ui.model.json.JSONModel();
		this.getView().setModel(topicmodel, "topics");
		topicmodel.attachRequestFailed(function(event) {
			var text = event.getParameter("responseText");
			sap.m.MessageToast.show("Reading topics failed: " + text);
		});
		topicmodel.loadData("../rest/topics");
		this.onSampleRefresh();
	},
	onSampleRefresh : function(event) {
		var samplemodel = new sap.ui.model.json.JSONModel();
		var samplecontrol = this.getView().byId("samplefile");
		samplecontrol.setModel(samplemodel, "samplefiles");
		var subjectname = jQuery.sap.getUriParameters().get("subject");
		samplemodel.loadData("../rest/sample/" + encodeURI(subjectname));
	},
	onAddTab : function(event) {
		var model = this.getView().getModel();
		var steps = model.getProperty("/rulesteps");
		if (steps && steps.length > 0) {
			var rules = {
				"type" : "RuleStep",
				"fieldname" : "next step",
				"rules" : [ ]
			}; 
			this.addDefaultRules(steps[0]["rules"], rules["rules"]);
			model.setProperty("/rulesteps/" + steps.length, rules);
		} else {
			var subjectname = jQuery.sap.getUriParameters().get("subject");
			model.loadData("../rest/subjects/" + encodeURI(subjectname) + "/defaultrule");
		}
	},
	onCloseTab : function(event) {
		var item = event.getParameter("item");
		var path = item.getBindingContext().getPath();
		var index = path.substring(path.lastIndexOf('/')+1);
		var model = this.getView().getModel();
		var steps = model.getProperty("/rulesteps");
		steps.pop(index);
		model.setProperty("/rulesteps", steps);
	},
	addDefaultRules : function(from, to) {
		for ( var r of from) {
			if (r["type"] === "RecordRule" || r["type"] === "GenericRules" || r["type"] === "ArrayRule") {
				var n = [];
				to.push({
					"type" : r["type"],
					"fieldname" : r["fieldname"],
					"rules" : n
				});
				this.addDefaultRules(r["rules"], n);
			} else {
				to.push({
					"type" : "EmptyRule",
					"fieldname" : r["fieldname"],
					"fielddatatype" : r["fielddatatype"]
				});
			}
		}
	},
	resulttypeFormatter : function(resulttype) {
		if (resulttype === "PASS") {
			return "sap-icon://message-success";
		} else if (resulttype === "FAIL") {
			return "sap-icon://message-warning";
		} else if (resulttype === "WARN") {
			return "sap-icon://question-mark";
		} else {
			return "sap-icon://message-error";
		}
	},
	onChangeRuleType: function(event) {
		var targetmodel = this.getView().getModel();
		var rulepath = event.getParameter("selectedItem").getBindingContext().getPath();
		var targetrow = targetmodel.getProperty(rulepath);
		var ruletype = targetrow.type;
		var parentpath = rulepath.substring(0, rulepath.lastIndexOf("/rules/"));
		if (parentpath && parentpath.length > 0) {
			/*
			 * Within a TestSet, there should be always at least on additional item to enter a new rule
			 */
			var record = targetmodel.getProperty(parentpath);
			if (record.type.startsWith("TestSet")) {
				if (record.rules && record.rules.length > 0) {
					var oLastEntity = record.rules[record.rules.length-1];
					if (oLastEntity.type !== "EmptyRule") {
						targetmodel.setProperty(parentpath + "/rules/" + record.rules.length,
							{fieldname: targetrow.fieldname, type: "EmptyRule", iffalse: "FAIL", sampleresult: null});
					}
				}
			}
		}
		if (ruletype === 'EmptyRule') {
			targetmodel.setProperty(rulepath + "/rules", null);
		} else if (ruletype === 'PrimitiveRule') {
			targetmodel.setProperty(rulepath + "/rules", null);
			if (!!!targetrow.iffalse) {
			    targetmodel.setProperty(rulepath + "/iffalse", "FAIL");
			}
			targetmodel.setProperty(rulepath + "/sampleresult", null);
		} else {
			var childrules = targetmodel.getProperty(rulepath + "/rules");
			if (!childrules || childrules.length === 0) {
				targetmodel.setProperty(rulepath + "/rules",
						[ { fieldname: targetrow.fieldname, type: 'PrimitiveRule', iffalse: "FAIL", sampleresult: null },
						 { fieldname: targetrow.fieldname, type: 'EmptyRule', iffalse: "FAIL", sampleresult: null } ] );
			}
		}
	},
	onSave : function(event) {
		var rulename = jQuery.sap.getUriParameters().get("rule");
		var model = this.getView().getModel();
		var newrulename = model.getProperty("/name");
		var post = new sap.ui.model.json.JSONModel();
		post.attachRequestCompleted(function(event) {
			console.log(post.getProperty("/"));
			if (event.getParameter("success")) {
				if (rulename !== newrulename) {
					const url = new URL(window.location)
					url.searchParams.set("rule", newrulename);
					url.searchParams.delete("new");
					history.pushState(null, '', url);
				}
				sap.m.MessageToast.show("Saved");
			}
		});
		post.attachRequestFailed(function(event) {
			var text = event.getParameter("responseText");
			sap.m.MessageToast.show("Saving failed: " + text);
		});
		var subjectname = jQuery.sap.getUriParameters().get("subject");
		if (!rulename) {
			rulename = "new";
		}
		var json = JSON.stringify(model.getProperty("/"));
		var headers = {
			"Content-Type": "application/json;charset=utf-8"
		}
		post.loadData("../rest/subjects/" + encodeURI(subjectname) + "/rules/" + rulename, json, true, "POST", false, true, headers);
	},
	onActivate : function() {
		var rulename = jQuery.sap.getUriParameters().get("rule");
		var post = new sap.ui.model.json.JSONModel();
		post.attachRequestCompleted(function(event) {
			console.log(post.getProperty("/"));
			if (event.getParameter("success")) {
				sap.m.MessageToast.show("Activated");
			}
		});
		post.attachRequestFailed(function(event) {
			var text = event.getParameter("responseText");
			sap.m.MessageToast.show("Activate failed: " + text);
		});
		var subjectname = jQuery.sap.getUriParameters().get("subject");
		var headers = {
			"Content-Type": "application/json;charset=utf-8"
		}
		post.loadData("../rest/subjects/" + encodeURI(subjectname) + "/rules/" + rulename, undefined, true, "PATCH", false, true, headers);
	},
	onConditionChange: function(event) {
		var source = event.getSource();
		var rulepath = source.getBindingContext().getPath();
		var editor = this.getView().byId("formulaeditor");
		editor.setValue(event.getParameter("value"));
		editor.setEditable(true);
		editor.data("path", rulepath + "/condition");
		var label = this.getView().byId("formulaeditorlabel");
		label.setText("Rule Formula for: " + rulepath);
	},
	onSubstituteChange: function(event) {
		var source = event.getSource();
		var rulepath = source.getBindingContext().getPath();
		var editor = this.getView().byId("formulaeditor");
		editor.setValue(event.getParameter("value"));
		editor.setEditable(true);
		editor.data("path", rulepath + "/substitute");
		var label = this.getView().byId("formulaeditorlabel");
		label.setText("Rule Formula for: " + rulepath);
	},
	onEditorChange: function(event) {
		var source = event.getSource();
		var rulepath = source.data("path")
		if (rulepath) {
			var model = this.getView().getModel();
			model.setProperty(rulepath, event.getParameter("value"));
		}
	},
	onReadSample : function(event) {
		var model = this.getView().getModel();
		if (model.getProperty("/samplefile")) {
			var json = JSON.stringify(model.getProperty("/"));
			var headers = {
				"Content-Type": "application/json;charset=utf-8"
			}
			model.loadData("../rest/calculate", json, true, "POST", false, true, headers);
		} else {
			sap.m.MessageToast.show("No sample file to read from was selected");
		}
	},
	onSampleReapply : function(event) {
		this.onReadSample(event);
	},
	onStepCalc : function(event) {
		var model = this.getView().getModel();
		var source = event.getSource().getParent().getParent();
		var steppath = source.getBindingContext().getPath(); // i.e. steppath: /rulesteps/0
		var stepindex = steppath.substring(steppath.lastIndexOf('/') + 1);
		var json = JSON.stringify(model.getProperty("/"));
		var headers = {
			"Content-Type": "application/json;charset=utf-8"
		}
		var stepmodel = new sap.ui.model.json.JSONModel();
		stepmodel.attachRequestFailed(function(event) {
			var text = event.getParameter("responseText");
			sap.m.MessageToast.show("Calculate failed: " + text);
		});
		stepmodel.attachRequestCompleted(function(event) {
			var successful = event.getParameter("success");
			if (successful) {
				model.setProperty("/rulesteps/" + stepindex + "/", stepmodel.getProperty("/"));
				sap.m.MessageToast.show("Recalculated");
			}
		});
		stepmodel.loadData("../rest/calculate/" + stepindex, json, true, "POST", false, true, headers);
	},
	onMoveLeft : function(event) {
		var source = event.getSource().getParent().getParent();
		var steppath = source.getBindingContext().getPath(); // i.e. steppath: /rulesteps/0
		var stepindex = parseInt(steppath.substring(steppath.lastIndexOf('/') + 1));
		if (stepindex > 0) {
			var model = this.getView().getModel();
			var steps = model.getProperty("/rulesteps");
			steps.splice(stepindex-1, 0, steps.splice(stepindex, 1)[0]);
			model.setProperty("/rulesteps", steps);
		}
	},
	onMoveRight : function(event) {
		var source = event.getSource().getParent().getParent();
		var steppath = source.getBindingContext().getPath(); // i.e. steppath: /rulesteps/0
		var stepindex = parseInt(steppath.substring(steppath.lastIndexOf('/') + 1));
		var model = this.getView().getModel();
		var steps = model.getProperty("/rulesteps");
		if (stepindex < steps.length-1) {
			steps.splice(stepindex+1, 0, steps.splice(stepindex, 1)[0]);
			model.setProperty("/rulesteps", steps);
		}
	}
});
});


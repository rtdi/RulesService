sap.ui.define(["sap/ui/core/mvc/Controller"],
	function(Controller) {
		"use strict";
		return Controller.extend("io.rtdi.bigdata.rulesservice.ui5.Sample", {
			onInit: function() {
				var model = new sap.ui.model.json.JSONModel();
				this.getView().setModel(model);
				model.attachRequestFailed(function(event) {
					var text = event.getParameter("responseText");
					sap.m.MessageToast.show("Reading topics failed: " + text);
				});
				model.loadData("../rest/topics");
				var table = this.getView().byId("tabledata");
				var tablemodel = new sap.ui.model.json.JSONModel();
				table.setModel(tablemodel);
			},
			onExecute: function(event) {
				var table = this.getView().byId("tabledata");
				var model = table.getModel();
				var topicselector = this.getView().byId("topicselector");
				var selectedtopics = topicselector.getSelectedKeys();
				if (!selectedtopics || selectedtopics.length === 0) {
					sap.m.MessageToast.show("No input topic(s) selected");
				} else {
					fetch("../rest/sample", {
						method: "POST",
						body: JSON.stringify(selectedtopics),
						headers: {
							"Content-Type": "application/json",
						}
					})
					.then((response) => {
						if (response.ok) {
							return response;
						} else {
							return Promise.reject(response);
						}
					})
					.then(response => {
						const reader = response.body.getReader();
						var utf8decoder = new TextDecoder('utf-8');
						var messages = [];
						model.setProperty("/", []);
						var line = "";
						return new ReadableStream({
							start(controller) {
								function update() {
									reader.read().then(({ done, value }) => {
										if (done) {
											controller.close();
											return;
										}
										let chunk = utf8decoder.decode(value);
										var pos;
										do {
											pos = chunk.indexOf("\n");
											if (pos >= 0) {
												line = line + chunk.substring(0, pos);
												var data = JSON.parse(line);
												if (data.hasOwnProperty("offset")) {
													model.setProperty("/" + messages.length, data);
													messages.push(data);
												}
												line = "";
												chunk = chunk.substring(pos+1);
											} else {
												line = line + chunk;
											}
											
										} while (pos >= 0);
										update();
									});
								}
								update();
							}
						});
					})
					.catch((err) => err.text()
						.then((s) => {
							try {
								var json = JSON.parse(s);
								sap.m.MessageToast.show(json["errormessage"]);
							} catch (parseerror) {
								sap.m.MessageToast.show(s);
							}
						})
					);
				}
			},
			onSavePayload: function(event) {
				var control = event.getSource().getParent();
				var path = control.getBindingContext().getPath();
				var model = control.getModel();
				var i = path.substring(path.lastIndexOf('/')+1);
				this.savepayload(model, i);
			},
			onSaveSelected: function(event) {
				var table = this.getView().byId("tabledata");
				var model = table.getModel();
				var data = model.getProperty("/");
				for ( var i=0; i<data.length; i++) {
					var targetrow = data[i];
					if (targetrow["selected"]) {
						this.savepayload(model, i);
					}
				}
			},
			savepayload: function(model, i) {
				var data = model.getProperty("/");
				var targetrow = data[i];
				var json = JSON.stringify(targetrow);
				var headers = {
					"Content-Type": "application/json;charset=utf-8"
				}
				var post = new sap.ui.model.json.JSONModel();
				post.attachRequestCompleted(function(jsonevent) {
					if (jsonevent.getParameter("success")) {
						model.setProperty("/" + i + "/filename", post.getProperty("/filename"));
						model.setProperty("/" + i + "/error", undefined);
					} else {
						model.setProperty("/" + i + "/error", jsonevent.getParameter("errorobject"));
					}
				});
				post.loadData("../rest/sample/" + targetrow["subjectname"], json, true, "POST", false, true, headers);
			}
		});
	});


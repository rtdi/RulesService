sap.ui.define([
	'sap/m/Input',
	'./library'
], function(Input) {
  return Input.extend("io.rtdi.bigdata.rulesservice.ui5.lib.FocusInput", {
		metadata : {
			properties : {
			},
			aggregations : {
			},
			events : {
				"focusIn" : {"value": {type: "string"}},
				"focusOut" : {"value": {type: "string"}},
			}
		},
		renderer : {},
		onfocusin : function() {
			this.fireFocusIn({ "value" : this.getValue()});
		},
		onfocusout : function() {
			this.fireFocusOut({ "value" : this.getValue()});
		},
	});
});

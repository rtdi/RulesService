sap.ui.define([
	'sap/m/Input',
	'./library'
], function(Input) {
  return Input.extend("io.rtdi.bigdata.rulesservice.lib.FocusInput", {
		metadata : {
			properties : {
			},
			aggregations : {
			},
			events : {
				"focusIn" : {"value": {type: "string"}}
			}
		},
		renderer : {},
		onfocusin : function() {
			this.fireFocusIn({ "value" : this.getValue()});
		},
	});
});

sap.ui.define([
	'sap/ui/core/Lib',
	'sap/ui/core/library'
	], function(Library) {

	"use strict";

	/**
	 *
	 * @namespace
	 * @alias io.rtdi.bigdata.rulesservice.ui5.lib
	 * @author rtdi.io GmbH
	 * @public
	 */

	const thisLib = Library.init({
		version: "${version}",
		name : "io.rtdi.bigdata.rulesservice.ui5.lib",
		apiVersion: 2,
		dependencies : ["sap.ui.core"],
		types: [
		],
		interfaces: [],
		controls: [
			"io.rtdi.bigdata.rulesservice.ui5.lib.FocusInput",
			"io.rtdi.bigdata.rulesservice.ui5.lib.JSONViewer"
		],
		elements: []
	});

	return thisLib;
});
sap.ui.define([
	'sap/tnt/ToolPage',
	'./library'
], function(ToolPage) {
  return ToolPage.extend("io.rtdi.bigdata.rulesservice.ui5.lib.RulesToolPage", {
		metadata : {
			properties : {
			},
			aggregations : {
			},
			events : {
			}
		},
		renderer : {},
		init : function() {
			ToolPage.prototype.init.apply(this, arguments);
			this.setAggregation("sideContent",
				new sap.tnt.SideNavigation( {
					id : "sideNavigation",
					expanded: false,
					item : new sap.tnt.NavigationList ( {
						items: [
							new sap.tnt.NavigationListItem({
								text: "Home",
								icon: "sap-icon://home",
								href: "../index.html",
								tooltip: "Home screen"
							}),
							new sap.tnt.NavigationListItem({
								text: "Topics",
								icon: "sap-icon://inventory",
								href: "./Topics.html",
								tooltip: "Rules applied to topic data"
							}),
							new sap.tnt.NavigationListItem({
								text: "Rules",
								icon: "sap-icon://document-text",
								href: "./Rules.html",
								tooltip: "List or create rules for a subject"
							}),
							new sap.tnt.NavigationListItem({
								text: "Sample",
								icon: "sap-icon://download",
								href: "./Sample.html",
								tooltip: "Generate sample data"
							}),
							new sap.tnt.NavigationListItem({
								text: "Config",
								icon: "sap-icon://wrench",
								href: "./Config.html",
								tooltip: "Configure the connections"
							})
						]}
					)
				})
			);
		}
	});
});

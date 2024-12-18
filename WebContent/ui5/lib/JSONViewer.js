sap.ui.define(
[	'sap/ui/core/Control',
	'./library'
],
function(Control) {

	var counter = 0;

	/**
	 * Transform a json object into html representation
	 * 
	 * @return string
	 */
	function json2html(json, controlid, key, collapsed) {
		var html = '';
		if (typeof json === 'string') {
			// Escape tags and quotes
			json = json.replace(/&/g, '&amp;').replace(/</g,
					'&lt;').replace(/>/g, '&gt;').replace(/'/g,
					'&apos;').replace(/"/g, '&quot;');

			// Escape double quotes in the rendered non-URL
			// string.
			json = json.replace(/&quot;/g, '\\&quot;');
			if (key) {
				html += '<span class="json-key">"' + key + '"</span>:';
			}
			html += '<span class="json-string">"' + json + '"</span>';
		} else if (typeof json === 'number') {
			if (key) {
				html += '<span class="json-key">"' + key + '"</span>:';
			}
			html += '<span class="json-literal">' + json + '</span>';
		} else if (typeof json === 'boolean') {
			if (key) {
				html += '<span class="json-key">"' + key + '"</span>:';
			}
			html += '<span class="json-literal">' + json + '</span>';
		} else if (json === null) {
			if (key) {
				html += '<span class="json-key">"' + key + '"</span>:';
			}
			html += '<span class="json-literal">null</span>';
		} else if (json instanceof Array) {
			if (key) {
				html += '<span class="json-key-array">↕"' + key + '"</span>:';
			}
			if (json.length > 0) {
				var placeholder = json.length + (json.length > 1 ? ' items' : ' item');
				html += '[<ol class="json-array" style="display: ' + (collapsed && key ? "none" : "block") + ';">';
				counter++;
				for (var i = 0; i < json.length; ++i) {
					html += '<li>';
					html += json2html(json[i], controlid, undefined, collapsed);
					// Add comma if item is not last
					if (i < json.length - 1) {
						html += ',';
					}
					html += '</li>';
				}
				html += '</ol>';
				html += '<span class="json-placeholder" style="display: ' + (collapsed && key ? "inline" : "none") + ';">' + placeholder + '</span>';
				html += ']';
			} else {
				html += '[]';
			}
		} else if (typeof json === 'object') {
			if (key) {
				html += '<span class="json-key-record">↕"' + key + '"</span>:';
			}
			var keyCount = Object.keys(json).length;
			var placeholder = keyCount + (keyCount > 1 ? ' items' : ' item');
			if (keyCount > 0) {
				html += '{<ul class="json-dict" style="display: ' + (collapsed && key ? "none" : "block") + ';">';
				for ( var childkey in json) {
					if (Object.prototype.hasOwnProperty.call(json, childkey)) {
						html += '<li>';
						html += json2html(json[childkey], controlid, childkey, collapsed);
						// Add comma if item is not last
						if (--keyCount > 0) {
							html += ',';
						}
						html += '</li>';
					}
				}
				html += '</ul>';
				html += '<span class="json-placeholder" style="display: ' + (collapsed && key ? "inline" : "none") + ';">' + placeholder + '</span>';
				html += '}';
			} else {
				html += '{}';
			}
		}
		return html;
	};

      
	var control = Control.extend(
		"io.rtdi.bigdata.rulesservice.lib.JSONViewer",
		{
			metadata : {
				properties : {
					text : {
						type : "string",
						defaultValue : ""
					},
					collapsed : {
						type : "boolean",
						defaultValue : false
					}
				},
				aggregations : {},
			},

			renderer : function(oRm, oControl) {
				oRm.write("<div");
				oRm.writeControlData(oControl);
				oRm.write("><div class='JSONViewerDIV'>"); // will carry the scrollbar

				var outputtext = "";
				var inputtext = oControl.getText();
				if (inputtext) {
					var json = JSON.parse(inputtext);
					outputtext = json2html(json, oControl.getId(), undefined, oControl.getCollapsed());
				}

				oRm.write(outputtext);

				oRm.write("</div></div>")
			},

			init : function() {
			},

			onAfterRendering : function(arguments) {
				if (sap.ui.core.Control.prototype.onAfterRendering) {
					sap.ui.core.Control.prototype.onAfterRendering.apply(this, arguments);
				}
			},

		});
	
	control.prototype.onclick = function (oEvent) {
		var target1 = $(oEvent.target).siblings('ul.json-dict, ol.json-array');
		var target2 = $(oEvent.target).siblings('.json-placeholder');
		if (target1.length == 0 || target2.length == 0) {
			// ignore
		} else if (target1[0].style.display === 'none') {
			target1[0].style.display = 'block';
			target2[0].style.display = 'none';
		} else {
			target1[0].style.display = 'none';
			target2[0].style.display = 'inline';
		}
		return false;
	};

	return control;
});

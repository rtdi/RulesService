package io.rtdi.bigdata.rulesservice;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.MicroServiceTransformation;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;

public class RuleStep extends MicroServiceTransformation {
	private Map<String, SchemaRuleSet> schemarules = new HashMap<>();

	public RuleStep(String name) {
		super(name);
	}

	@Override
	public JexlRecord applyImpl(JexlRecord valuerecord) throws IOException {
		String schemaname = valuerecord.getSchema().getName();
		SchemaRuleSet ruleset = schemarules.get(schemaname);
		if (ruleset == null) {
			return valuerecord;
		} else {
			return ruleset.apply(valuerecord);
		}
	}

	public SchemaRuleSet createSchemaRuleSet(String schema) {
		SchemaRuleSet set = new SchemaRuleSet(schema);
		schemarules.put(schema, set);
		return set;
	}

	public Map<String, SchemaRuleSet> getSchemaRules() {
		return schemarules;
	}

	public void addSchemaRuleSet(SchemaRuleSet s) {
		schemarules.put(s.getSchemaname(), s);
	}

	public SchemaRuleSet getSchemaRuleOrFail(String schemaname) throws ConnectorCallerException {
		if (schemarules != null) {
			SchemaRuleSet r = schemarules.get(schemaname);
			if (r != null) {
				return r;
			}
		}
		throw new ConnectorCallerException("No RuleSet found for this schema", null, schemaname);
	}
}

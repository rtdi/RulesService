package io.rtdi.bigdata.rulesservice;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import io.rtdi.bigdata.connector.pipeline.foundation.MicroServiceTransformation;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaRegistryName;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.pipeline.foundation.utils.FileNameEncoder;
import io.rtdi.bigdata.connector.pipeline.foundation.utils.SchemaNameEncoder;

public class RuleStep extends MicroServiceTransformation {
	/*
	 * Index over the encoded schema name
	 */
	private Map<String, SchemaRuleSet> rs = new HashMap<>();

	public RuleStep(String name) {
		super(name);
	}

	public RuleStep(File dir) throws PropertiesException {
		this(dir.getName());
		for (File rulefile : dir.listFiles()) {
			if (rulefile.isFile() && rulefile.getName().endsWith(".json")) {
				SchemaRegistryName schemaname = SchemaRegistryName.create(rulefile.getName().substring(0, rulefile.getName().length()-5));
				SchemaRuleSet r = new SchemaRuleSet(schemaname, dir);
				rs.put(SchemaNameEncoder.encodeName(schemaname.getEncodedName()), r);
			}
		}
	}

	@Override
	public JexlRecord applyImpl(JexlRecord valuerecord) throws IOException {
		SchemaRuleSet r = rs.get(valuerecord.getSchema().getFullName());
		if (r != null && r.getRules() != null) {
			addOperationLogLine("Applied rule for schema " + r.getSchemaname());
			return r.apply(valuerecord);
		} else {
			addOperationLogLine("Record with schema " + valuerecord.getSchema().getFullName() + " has no rules defined - data passed through");
			return valuerecord;
		}
	}

	public SchemaRuleSet getSchemaRule(SchemaRegistryName schemaname) {
		return rs.get(schemaname.getEncodedName());
	}

	public void setSchemaRuleSet(Map<String, SchemaRuleSet> data) {
		rs = data;
	}

	public Map<String, SchemaRuleSet> getSchemaRules() {
		return rs;
	}

	public void addSchemaRuleSet(SchemaRuleSet r) {
		rs.put(r.getSchemaName().getEncodedName(), r);
	}

	public void removeSchemaRuleSet(SchemaRegistryName schemaname) {
		rs.remove(schemaname.getEncodedName());
	}

	public void deleteSchemaRuleSetDefinition(SchemaRegistryName schemaname, File dir) {
		rs.remove(schemaname.getEncodedName());
		File file = new File(dir, FileNameEncoder.encodeName(schemaname.getName()) + ".json");
		if (file.exists()) {
			file.delete();
		}
	}

}

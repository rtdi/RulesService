package io.rtdi.bigdata.rulesservice;

import java.io.File;
import java.io.IOException;

import io.rtdi.bigdata.connector.pipeline.foundation.MicroServiceTransformation;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

public class RuleStep extends MicroServiceTransformation {
	private SchemaRuleSet rs;

	public RuleStep(String name) {
		super(name);
		rs = new SchemaRuleSet();
	}

	public RuleStep(File dir) throws PropertiesException {
		this(dir.getName());
		rs = new SchemaRuleSet(dir);
	}

	@Override
	public JexlRecord applyImpl(JexlRecord valuerecord) throws IOException {
		if (rs.getRules() != null) {
			return rs.apply(valuerecord);
		} else {
			return valuerecord;
		}
	}

	public SchemaRuleSet getSchemaRule() {
		return rs;
	}

	public void setSchemaRuleSet(SchemaRuleSet data) {
		rs = data;
	}

}

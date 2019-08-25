package io.rtdi.bigdata.rulesservice.rules;

import org.apache.avro.Schema;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

public abstract class TestSet extends RuleWithName<PrimitiveRule> {

	public TestSet() {
		super();
	}

	public TestSet(String fieldname, String rulename) {
		super(fieldname, rulename);
	}

	public TestSet(String fieldname, String rulename, PrimitiveRule rule) {
		super(fieldname, rulename);
		addRule(rule);
	}

	protected void copyTests(TestSet t, Schema fieldschema) throws PropertiesException {
		for (PrimitiveRule r : getRules()) {
			t.addRule(r.createUIRuleTree(fieldschema));
		}
	}

}

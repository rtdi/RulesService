package io.rtdi.bigdata.rulesservice.rules;

import org.apache.avro.Schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown=true)
public abstract class TestSet extends RuleWithName {

	public TestSet() {
		super();
	}

	public TestSet(String fieldname, String rulename, Schema schema) {
		super(fieldname, rulename, schema);
	}

	public TestSet(String fieldname, String rulename, Rule rule, Schema schema) {
		super(fieldname, rulename, schema);
		addRule(rule);
	}

}

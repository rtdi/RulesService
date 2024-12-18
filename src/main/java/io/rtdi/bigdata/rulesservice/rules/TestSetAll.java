package io.rtdi.bigdata.rulesservice.rules;

import java.io.IOException;

import org.apache.avro.Schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.rtdi.bigdata.connector.pipeline.foundation.avro.AvroJexlContext;
import io.rtdi.bigdata.kafka.avro.RuleResult;

@JsonIgnoreProperties(ignoreUnknown=true)
public class TestSetAll extends TestSet {

	public TestSetAll() {
		super();
	}

	public TestSetAll(String fieldname, String rulename, Schema schema) {
		super(fieldname, rulename, schema);
	}

	public TestSetAll(String fieldname, String rulename, Rule rule, Schema schema) {
		super(fieldname, rulename, rule, schema);
	}

	@Override
	public RuleResult apply(Object value, AvroJexlContext container, boolean test) throws IOException {
		setSampleValue(value, test);
		RuleResult result = RuleResult.PASS;
		for ( Rule rule : getRules()) {
			result = result.aggregate(rule.apply(value, container, test));
			if (test) {
				setSampleresult(result);
			}
		}
		return result;
	}

	@Override
	public String toString() {
		return getFieldname() + ": TestSetAll tests all child rules";
	}

}

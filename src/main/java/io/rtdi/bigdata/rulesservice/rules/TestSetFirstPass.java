package io.rtdi.bigdata.rulesservice.rules;

import java.io.IOException;

import org.apache.avro.Schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.rtdi.bigdata.connector.pipeline.foundation.avro.AvroJexlContext;
import io.rtdi.bigdata.kafka.avro.RuleResult;

@JsonIgnoreProperties(ignoreUnknown=true)
public class TestSetFirstPass extends TestSet {

	public TestSetFirstPass() {
		super();
	}

	public TestSetFirstPass(String fieldname, String rulename, Schema schema) {
		super(fieldname, rulename, schema);
	}

	public TestSetFirstPass(String fieldname, String rulename, PrimitiveRule rule, Schema schema) {
		super(fieldname, rulename, rule, schema);
	}

	@Override
	public RuleResult apply(Object value, AvroJexlContext container, boolean test) throws IOException {
		setSampleValue(value, test);
		RuleResult result = RuleResult.FAIL;
		for ( Rule rule : getRules()) {
			result = rule.apply(value, container, test);
			if (result == RuleResult.PASS) {
				break;
			}
		}
		if (test) {
			setSampleresult(result);
		}
		return result;
	}

	@Override
	public String toString() {
		return getFieldname() + ": TestSetFirstPass tests until first PASS";
	}

}

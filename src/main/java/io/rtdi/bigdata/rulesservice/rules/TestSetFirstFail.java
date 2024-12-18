package io.rtdi.bigdata.rulesservice.rules;

import java.io.IOException;

import org.apache.avro.Schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.rtdi.bigdata.connector.pipeline.foundation.avro.AvroJexlContext;
import io.rtdi.bigdata.kafka.avro.RuleResult;

@JsonIgnoreProperties(ignoreUnknown=true)
public class TestSetFirstFail extends TestSet {

	public TestSetFirstFail() {
		super();
	}

	public TestSetFirstFail(String fieldname, String rulename, Schema schema) {
		super(fieldname, rulename, schema);
	}

	public TestSetFirstFail(String fieldname, String rulename, Rule rule, Schema schema) {
		super(fieldname, rulename, rule, schema);
	}

	@Override
	public RuleResult apply(Object value, AvroJexlContext container, boolean test) throws IOException {
		setSampleValue(value, test);
		RuleResult result = RuleResult.PASS;
		for ( Rule rule : getRules()) {
			RuleResult currentresult = rule.apply(value, container, test);
			if (currentresult == RuleResult.FAIL) {
				result = RuleResult.FAIL;
				break;
			} else {
				result = result.aggregate(currentresult);
			}
		}
		if (test) {
			setSampleresult(result);
		}
		return result;
	}

	@Override
	public String toString() {
		return getFieldname() + ": TestSetFirstFail tests until first FAIL";
	}

}

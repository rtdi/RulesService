package io.rtdi.bigdata.rulesservice.rules;

import java.io.IOException;

import org.apache.avro.Schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.rtdi.bigdata.connector.pipeline.foundation.avro.AvroJexlContext;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlRecord;
import io.rtdi.bigdata.kafka.avro.RuleResult;

/**
 * This is a rule set no tied to a single field
 */
@JsonIgnoreProperties(ignoreUnknown=true)
public class GenericRules extends TestSet {

	public GenericRules() {
		super("(more)", null, null);
	}

	public GenericRules(Rule rule, Schema schema) {
		super("(more)", null, rule, schema);
	}

	@Override
	public RuleResult apply(Object value, AvroJexlContext container, boolean test) throws IOException {
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
		return getFieldname() + ": Generic rule tests all child rules";
	}

	@Override
	public void updateSampleOutput(JexlRecord valuerecord) {
		// Generic rules do not have fields and hence nothing to output as sample value
	}

}

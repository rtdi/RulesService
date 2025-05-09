package io.rtdi.bigdata.rulesservice.rules;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.rtdi.bigdata.kafka.avro.RuleResult;
import io.rtdi.bigdata.rulesservice.jexl.AvroContainer;
import io.rtdi.bigdata.rulesservice.jexl.JexlRecord;

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
	public RuleResult apply(Object value, AvroContainer container, boolean test) throws IOException {
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

	@Override
	public Rule clone() {
		GenericRules ret = new GenericRules();
		ret.setDataType(getDataType());
		ret.setFieldname(getFieldname());
		ret.setRulename(getRulename());
		ret.setSchemaname(getSchemaname());
		if (getRules() != null) {
			List<Rule> a = new ArrayList<>(getRules().size());
			ret.setRules(a);
			for (Rule r : getRules()) {
				a.add(r.clone());
			}
		}
		return ret;
	}

}

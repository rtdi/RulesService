package io.rtdi.bigdata.rulesservice.rules;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.rtdi.bigdata.kafka.avro.RuleResult;
import io.rtdi.bigdata.rulesservice.jexl.AvroContainer;

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
	public RuleResult apply(Object value, AvroContainer container, boolean test) throws IOException {
		setSampleValue(value, test);
		RuleResult result = RuleResult.PASS;
		for ( Rule rule : getRules()) {
			if (!(rule instanceof EmptyRule)) {
				result = result.aggregate(rule.apply(value, container, test));
				if (test) {
					setSampleresult(result);
				}
			}
		}
		return result;
	}

	@Override
	public String toString() {
		return getFieldname() + ": TestSetAll tests all child rules";
	}

	@Override
	public Rule clone() {
		TestSetAll ret = new TestSetAll();
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

package io.rtdi.bigdata.rulesservice.rules;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.rtdi.bigdata.kafka.avro.RuleResult;
import io.rtdi.bigdata.rulesservice.jexl.AvroContainer;

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
	public RuleResult apply(Object value, AvroContainer container, boolean test) throws IOException {
		setSampleValue(value, test);
		RuleResult result = RuleResult.PASS;
		for ( Rule rule : getRules()) {
			if (!(rule instanceof EmptyRule)) {
				RuleResult currentresult = rule.apply(value, container, test);
				if (currentresult == RuleResult.FAIL) {
					result = RuleResult.FAIL;
					break;
				} else {
					result = result.aggregate(currentresult);
				}
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
	@Override
	public Rule clone() {
		TestSetFirstFail ret = new TestSetFirstFail();
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

package io.rtdi.bigdata.rulesservice.rules;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.rtdi.bigdata.kafka.avro.RuleResult;
import io.rtdi.bigdata.rulesservice.jexl.AvroContainer;

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
	public RuleResult apply(Object value, AvroContainer container, boolean test) throws IOException {
		setSampleValue(value, test);
		RuleResult result = RuleResult.FAIL;
		for ( Rule rule : getRules()) {
			if (!(rule instanceof EmptyRule)) {
				result = rule.apply(value, container, test);
				if (result == RuleResult.PASS) {
					break;
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
		return getFieldname() + ": TestSetFirstPass tests until first PASS";
	}

	@Override
	public Rule clone() {
		TestSetFirstPass ret = new TestSetFirstPass();
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

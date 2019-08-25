package io.rtdi.bigdata.rulesservice.rules;

import java.util.List;

import org.apache.avro.Schema;

import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.RuleResult;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

public class TestSetFirstFail extends TestSet {

	public TestSetFirstFail() {
		super();
	}

	public TestSetFirstFail(String fieldname, String rulename) {
		super(fieldname, rulename);
	}

	public TestSetFirstFail(String fieldname, String rulename, PrimitiveRule rule) {
		super(fieldname, rulename, rule);
	}

	@Override
	public RuleResult apply(JexlRecord valuerecord, List<JexlRecord> ruleresults) {
		RuleResult result = RuleResult.PASS;
		for ( PrimitiveRule rule : getRules()) {
			RuleResult currentresult = rule.apply(valuerecord, ruleresults);
			if (currentresult == RuleResult.FAIL) {
				return result;
			} else {
				result = result.aggregate(currentresult);
			}
		}
		return result;
	}

	@Override
	public String toString() {
		return "TestSetFirstFail \"" + getRulename() + "\" tests until first FAILs";
	}

	@Override
	protected TestSetFirstFail createUIRuleTree(Schema fieldschema) throws PropertiesException {
		TestSetFirstFail t = new TestSetFirstFail(getFieldname(), getRulename());
		copyTests(t, fieldschema);
		return t;
	}

}

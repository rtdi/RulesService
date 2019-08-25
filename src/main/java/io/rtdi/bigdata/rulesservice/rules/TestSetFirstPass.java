package io.rtdi.bigdata.rulesservice.rules;

import java.util.List;

import org.apache.avro.Schema;

import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.RuleResult;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

public class TestSetFirstPass extends TestSet {

	public TestSetFirstPass() {
		super();
	}

	public TestSetFirstPass(String fieldname, String rulename) {
		super(fieldname, rulename);
	}

	public TestSetFirstPass(String fieldname, String rulename, PrimitiveRule rule) {
		super(fieldname, rulename, rule);
	}

	@Override
	public RuleResult apply(JexlRecord valuerecord, List<JexlRecord> ruleresults) {
		RuleResult result = RuleResult.FAIL;
		for ( PrimitiveRule rule : getRules()) {
			result = rule.apply(valuerecord, ruleresults);
			if (result == RuleResult.PASS) {
				return result;
			}
		}
		return result;
	}

	@Override
	public String toString() {
		return "TestSetFirstPass \"" + getRulename() + "\" tests until first PASSes";
	}

	@Override
	protected TestSetFirstPass createUIRuleTree(Schema fieldschema) throws PropertiesException {
		TestSetFirstPass t = new TestSetFirstPass(getFieldname(), getRulename());
		copyTests(t, fieldschema);
		return t;
	}

}

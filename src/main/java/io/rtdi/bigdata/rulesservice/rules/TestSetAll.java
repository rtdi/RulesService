package io.rtdi.bigdata.rulesservice.rules;

import java.util.List;

import org.apache.avro.Schema;

import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.RuleResult;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

public class TestSetAll extends TestSet {

	public TestSetAll() {
		super();
	}

	public TestSetAll(String fieldname, String rulename) {
		super(fieldname, rulename);
	}

	public TestSetAll(String fieldname, String rulename, PrimitiveRule rule) {
		super(fieldname, rulename, rule);
	}

	@Override
	public RuleResult apply(JexlRecord valuerecord, List<JexlRecord> ruleresults) {
		RuleResult result = RuleResult.PASS;
		for ( PrimitiveRule rule : getRules()) {
			result = result.aggregate(rule.apply(valuerecord, ruleresults));
		}
		return result;
	}

	@Override
	public String toString() {
		return "TestSetAll \"" + getRulename() + "\" tests all child rules";
	}

	@Override
	protected TestSetAll createUIRuleTree(Schema fieldschema) throws PropertiesException {
		TestSetAll t = new TestSetAll(getFieldname(), getRulename());
		copyTests(t, fieldschema);
		return t;
	}

}

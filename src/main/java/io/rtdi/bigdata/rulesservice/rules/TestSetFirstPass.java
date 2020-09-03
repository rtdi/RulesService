package io.rtdi.bigdata.rulesservice.rules;

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.commons.jexl3.JexlException;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.RuleResult;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

@JsonIgnoreProperties(ignoreUnknown=true)
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
	public RuleResult apply(JexlRecord valuerecord, List<JexlRecord> ruleresults) throws IOException {
		RuleResult result = RuleResult.FAIL;
		for ( Rule rule : getRules()) {
			result = rule.apply(valuerecord, ruleresults);
			if (result == RuleResult.PASS) {
				return result;
			}
		}
		return result;
	}
	
	@Override
	public RuleResult validateRule(JexlRecord valuerecord) throws JexlException {
		/*
		 * Needs to go through all rules instead of exit early, else the other rules are not evaluated and hence their result is not displayed. 
		 */
		RuleResult r = RuleResult.FAIL;
		for ( Rule rule : getRules()) {
			RuleResult intermediate = rule.validateRule(valuerecord);
			if (intermediate == RuleResult.PASS) {
				r = intermediate;
			}
		}
		sampleresult = r;
		return r;
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

	@Override
	protected Rule createNewInstance() throws ConnectorCallerException {
		return new TestSetFirstPass(getFieldname(), getRulename());
	}

}

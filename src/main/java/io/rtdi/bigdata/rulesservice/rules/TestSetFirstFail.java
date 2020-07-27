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
public class TestSetFirstFail extends TestSet {

	public TestSetFirstFail() {
		super();
	}

	public TestSetFirstFail(String fieldname, String rulename) {
		super(fieldname, rulename);
	}

	public TestSetFirstFail(String fieldname, String rulename, Rule rule) {
		super(fieldname, rulename, rule);
	}

	@Override
	public RuleResult apply(JexlRecord valuerecord, List<JexlRecord> ruleresults) throws IOException {
		RuleResult result = RuleResult.PASS;
		for ( Rule rule : getRules()) {
			RuleResult currentresult = rule.apply(valuerecord, ruleresults);
			if (currentresult == RuleResult.FAIL) {
				return RuleResult.FAIL;
			} else {
				result = result.aggregate(currentresult);
			}
		}
		return result;
	}
	
	@Override
	protected RuleResult validateRule(JexlRecord valuerecord) throws JexlException {
		/*
		 * Needs to evaluate all rules to display their result. A rule aggregation with Failed is always Failed, so we are fine here. 
		 */
		sampleresult = RuleResult.PASS;
		for ( Rule rule : getRules()) {
			RuleResult currentresult = rule.validateRule(valuerecord);
			if (currentresult == RuleResult.FAIL) {
				sampleresult = RuleResult.FAIL;
			} else if (sampleresult != null) {
				sampleresult = sampleresult.aggregate(currentresult);
			}
		}
		return sampleresult;
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

	@Override
	protected Rule createNewInstance() throws ConnectorCallerException {
		return new TestSetFirstFail(getFieldname(), getRulename());
	}

}

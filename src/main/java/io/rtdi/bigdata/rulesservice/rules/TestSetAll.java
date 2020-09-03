package io.rtdi.bigdata.rulesservice.rules;

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.RuleResult;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

@JsonIgnoreProperties(ignoreUnknown=true)
public class TestSetAll extends TestSet {

	public TestSetAll() {
		super();
	}

	public TestSetAll(String fieldname, String rulename) {
		super(fieldname, rulename);
	}

	public TestSetAll(String fieldname, String rulename, Rule rule) {
		super(fieldname, rulename, rule);
	}

	@Override
	public RuleResult apply(JexlRecord valuerecord, List<JexlRecord> ruleresults) throws IOException {
		RuleResult result = RuleResult.PASS;
		for ( Rule rule : getRules()) {
			result = result.aggregate(rule.apply(valuerecord, ruleresults));
		}
		return result;
	}

	@Override
	public RuleResult validateRule(JexlRecord valuerecord) {
		sampleresult = RuleResult.PASS;
		for ( Rule rule : getRules()) {
			RuleResult r = rule.validateRule(valuerecord);
			if (sampleresult != null) {
				sampleresult = sampleresult.aggregate(r);
			}
		}
		return sampleresult;
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

	@Override
	protected Rule createNewInstance() throws ConnectorCallerException {
		return new TestSetAll(getFieldname(), getRulename());
	}

}

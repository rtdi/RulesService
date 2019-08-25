package io.rtdi.bigdata.rulesservice.rules;

import java.util.List;

import org.apache.avro.Schema;

import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.RuleResult;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

public class EmptyRule extends Rule<Rule<?>> {

	public EmptyRule() {
		super();
	}
	
	public EmptyRule(String fieldname) {
		super(fieldname);
	}

	@Override
	public RuleResult apply(JexlRecord valuerecord, List<JexlRecord> ruleresults) {
		return null;
	}

	@Override
	protected Rule<?> createUIRuleTree(Schema fieldschema) throws PropertiesException {
		return this;
	}

}

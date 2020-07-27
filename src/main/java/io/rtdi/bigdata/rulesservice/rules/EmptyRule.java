package io.rtdi.bigdata.rulesservice.rules;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.commons.jexl3.JexlException;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.RuleResult;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

@JsonIgnoreProperties(ignoreUnknown=true)
public class EmptyRule extends Rule {

	private Object value;

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
	protected Rule createUIRuleTree(Schema fieldschema) throws PropertiesException {
		return this;
	}

	@Override
	void assignSamplevalue(JexlRecord sampledata) {
		this.value = sampledata.get(getFieldname());
	}

	@Override
	public Object getSamplevalue() throws PipelineCallerException {
		return PrimitiveRule.valueToJavaObject(value, getDataType());
	}

	@Override
	public RuleResult getSampleresult() throws PipelineCallerException {
		return null;
	}

	@Override
	protected RuleResult validateRule(JexlRecord valuerecord) throws JexlException {
		return null;
	}

	@Override
	protected Rule createSimplified() {
		/*
		 * An EmptyRule does never exist in the simplified version
		 */
		return null;
	}

	@Override
	protected Rule createNewInstance() {
		return null;
	}

}

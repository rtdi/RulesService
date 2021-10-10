package io.rtdi.bigdata.rulesservice.rules;

import org.apache.avro.Schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.kafka.avro.RuleResult;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

@JsonIgnoreProperties(ignoreUnknown=true)
public abstract class TestSet extends RuleWithName {
	private Object value;
	protected RuleResult sampleresult;

	public TestSet() {
		super();
	}

	public TestSet(String fieldname, String rulename) {
		super(fieldname, rulename);
	}

	public TestSet(String fieldname, String rulename, Rule rule) {
		super(fieldname, rulename);
		addRule(rule);
	}

	protected void copyTests(TestSet t, Schema fieldschema) throws PropertiesException {
		for (Rule r : getRules()) {
			t.addRule(r.createUIRuleTree(fieldschema));
		}
	}

	@Override
	public void assignSamplevalue(JexlRecord sampledata) {
		this.value = sampledata.get(getFieldname());
		if (this.getRules() != null) {
			for ( Rule r : this.getRules()) {
				r.assignSamplevalue(sampledata);
			}
		}
	}

	@Override
	public Object getSamplevalue() throws PipelineCallerException {
		return PrimitiveRule.valueToJavaObject(value, getDataType());
	}

	@Override
	public RuleResult getSampleresult() throws PipelineCallerException {
		return sampleresult;
	}

}

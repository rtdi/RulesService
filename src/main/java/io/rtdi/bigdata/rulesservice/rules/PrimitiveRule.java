package io.rtdi.bigdata.rulesservice.rules;

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.commons.jexl3.JexlException;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.IAvroDatatype;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.RuleResult;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.pipeline.foundation.mapping.PrimitiveMapping;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.ValueSchema;

@JsonIgnoreProperties(ignoreUnknown=true)
public class PrimitiveRule extends RuleWithName {
	private PrimitiveMapping condition;
	private RuleResult iffalse = RuleResult.PASS;
	private PrimitiveMapping substitute;
	private Object value;
	private RuleResult sampleresult;
	private Object substituteresult;
	private String conditionerror;
	private String substituteerror;

	public PrimitiveRule(String fieldname, String rulename, String condition, RuleResult iffalse, String substitute) throws IOException {
		super(fieldname, (rulename == null ? "Rule" : rulename));
		if (condition == null) {
			throw new ConnectorCallerException("Condition formula cannot be null", null, null, fieldname);
		}
		this.condition = new PrimitiveMapping(condition);
		this.iffalse = (iffalse == null?RuleResult.FAIL:iffalse);
		if (substitute != null) {
			this.substitute = new PrimitiveMapping(substitute);
		}
	}
	
	public PrimitiveRule() {
		super();
	}

	@Override
	public RuleResult apply(JexlRecord valuerecord, List<JexlRecord> ruleresults) throws IOException {
		try {
			RuleResult result = calculateResult(valuerecord);
			if (result != null) {
				JexlRecord r = ValueSchema.createAuditDetails();
				r.put(ValueSchema.AUDITTRANSFORMATIONNAME, getRulePath());
				r.put(ValueSchema.TRANSFORMRESULT, result.name());
				String v;
				String p = valuerecord.getPath(); 
				if (p == null) {
					v = getFieldname();
				} else {
					v = p + "." + getFieldname();
				}
				r.put(ValueSchema.AUDITTRANSFORMRESULTTEXT, v);
				int q = 100;
				switch (result) {
				case FAIL:
					q = 0;
					break;
				case WARN:
					q = 90;
					break;
				default:
					break;
				}
				r.put(ValueSchema.AUDIT_TRANSFORMRESULT_QUALITY, q);
				ruleresults.add(r);
	
				return result;
			} else {
				return RuleResult.PASS;				
			}
		} catch (JexlException e) {
			throw new ConnectorCallerException("Evaluationg the expression failed", e, "Check syntax and data", condition.toString());
		}
	}
	
	private RuleResult calculateResult(JexlRecord valuerecord) throws IOException {
		RuleResult r = null;
		Object o = condition.evaluate(valuerecord);
		if (o != null && o instanceof Boolean) {
			if (((Boolean) o).booleanValue()) {
				r = RuleResult.PASS;
			} else {
				r = iffalse;
			}
		} else {
			throw new ConnectorCallerException("This rule expression does not return true/false", null, "A condition must return true/false", condition.getExpression());
		}
		return r;
	}
	
	@Override
	protected RuleResult validateRule(JexlRecord valuerecord) {
		sampleresult = null;
		if (condition != null) {
			try {
				sampleresult = calculateResult(valuerecord);
				conditionerror = null;
			} catch (IOException e) {
				sampleresult = null;
				conditionerror = getResultString(condition.getExpression(), e);
			}
		}
		if (substitute != null) {
			try {
				substituteresult = substitute.evaluate(valuerecord);
				substituteerror = null;
			} catch (IOException e) {
				substituteresult = null;
				substituteerror = getResultString(substitute.getExpression(), e);
			}
		}
		return sampleresult;
	}

	public String getCondition() {
		if (condition == null) {
			return null;
		} else {
			return condition.toString();
		}
	}

	public void setCondition(String condition) {
		if (condition != null) {
			try {
				this.condition = new PrimitiveMapping(condition);
			} catch (IOException e) {
				sampleresult = null;
				conditionerror = getResultString(condition, e);
			}
		}
	}

	public RuleResult getIffalse() {
		return iffalse;
	}

	public void setIffalse(RuleResult iffalse) {
		this.iffalse = iffalse;
	}

	public String getSubstitute() {
		if (substitute != null) {
			return substitute.toString();
		} else {
			return null;
		}
	}

	public void setSubstitute(String substitute) {
		if (substitute != null) {
			try {
				this.substitute = new PrimitiveMapping(substitute);
			} catch (IOException e) {
				substituteresult = null;
				substituteerror = getResultString(substitute, e);
			}
		}
	}
	
	protected String getResultString(String formula, IOException e) {
		String m = e.getMessage();
		if (e.getCause() != null) {
			m = e.getCause().getMessage();
		}
		return formula + ":" + m;
	}



	@Override
	public String toString() {
		if (condition != null) {
			return "Rule: if (" + condition.toString() + ") then PASS else " + iffalse.name();
		} else {
			return "Rule: always PASS ";
		}
	}

	@Override
	protected PrimitiveRule createUIRuleTree(Schema fieldschema) throws PropertiesException {
		return this;
	}

	@Override
	public void assignSamplevalue(JexlRecord sampledata) {
		this.value = sampledata.get(getFieldname());
	}

	@Override
	public Object getSamplevalue() throws PipelineCallerException {
		return valueToJavaObject(value, getDataType());
	}
	
	static Object valueToJavaObject(Object value, IAvroDatatype datatype) throws PipelineCallerException {
		if (value != null && datatype != null) {
			return datatype.convertToJava(value);
		} else {
			return null;
		}
	}

	@Override
	public RuleResult getSampleresult() throws IOException {
		return sampleresult;
	}

	@Override
	protected Rule createSimplified() throws IOException {
		if (getCondition() != null) {
			return createNewInstance();
		} else {
			return null;
		}
	}

	@Override
	protected Rule createNewInstance() throws IOException {
		return new PrimitiveRule(getFieldname(), getRulename(), getCondition(), getIffalse(), getSubstitute());
	}

	public Object getSubstituteresult() {
		return substituteresult;
	}

	public String getSubstituteerror() {
		return substituteerror;
	}

	public String getConditionerror() {
		return conditionerror;
	}

}
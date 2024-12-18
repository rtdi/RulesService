package io.rtdi.bigdata.rulesservice.rules;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.commons.jexl3.JexlException;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.rtdi.bigdata.connector.pipeline.foundation.avro.AvroJexlContext;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.AvroRuleUtils;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlRecord;
import io.rtdi.bigdata.kafka.avro.RuleResult;
import io.rtdi.bigdata.kafka.avro.recordbuilders.ValueSchema;
import io.rtdi.bigdata.rulesservice.PropertiesException;

@JsonIgnoreProperties(ignoreUnknown=true)
public class PrimitiveRule extends RuleWithName {
	private PrimitiveMapping condition;
	private RuleResult iffalse = RuleResult.FAIL;
	private PrimitiveMapping substitute;
	private String conditionerror;
	private String substituteerror;

	public PrimitiveRule(String fieldname, String rulename, String condition, RuleResult iffalse, String substitute, Schema schema) throws IOException {
		super(fieldname, (rulename == null ? "Rule" : rulename), schema);
		if (condition == null) {
			throw new PropertiesException("Condition formula cannot be null", null, null, fieldname);
		}
		this.condition = new PrimitiveMapping(condition);
		this.iffalse = (iffalse == null?RuleResult.FAIL:iffalse);
		if (substitute != null && substitute.length() > 0) {
			this.substitute = new PrimitiveMapping(substitute);
		}
	}

	public PrimitiveRule() {
		super();
	}

	@Override
	public RuleResult apply(Object value, AvroJexlContext container, boolean test) throws IOException {
		try {
			setSampleValue(value, test);
			RuleResult result = calculateResult(container, test);
			if (result != null) {
				JexlRecord r = AvroRuleUtils.createAuditDetails();
				r.put(ValueSchema.AUDITTRANSFORMATIONNAME, getRulePath());
				r.put(ValueSchema.TRANSFORMRESULT, result.name());
				String v;
				String p = container.getPath();
				if (getFieldname() == null) {
					v = p;
				} else if (p == null) {
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
				container.addRuleresult(r);

			} else {
				result = RuleResult.PASS;
			}
			if (test) {
				setSampleresult(result);
				setConditionerror(null);
			}
			return result;
		} catch (JexlException e) {
			if (test) {
				setConditionerror(e.getMessage());
				return RuleResult.FAIL;
			} else {
				throw new PropertiesException("Evaluating the expression failed", e, "Check syntax and data", condition.toString());
			}
		}
	}

	private RuleResult calculateResult(AvroJexlContext valuerecord, boolean test) throws IOException {
		if (condition != null) {
			RuleResult r = null;
			Object o = condition.evaluate(valuerecord);
			if (o != null && o instanceof Boolean) {
				if (((Boolean) o).booleanValue()) {
					r = RuleResult.PASS;
				} else {
					r = iffalse;
					/*
					 * Calculate the substitute value
					 */
					if (substitute != null) {
						try {
							Object substitutevalue = substitute.evaluate(valuerecord);
							valuerecord.addChangedvalue(getFieldname(), substitutevalue);
							if (test) {
								setSampleoutput(Rule.valueToJavaObject(substitutevalue, getDataType()));
								setSubstituteerror(null);
							}
						} catch (IOException e) {
							if (test) {
								setSampleoutput(null);
								setSubstituteerror(getResultString(substitute.getExpression(), e));
							} else {
								throw e;
							}
						}
					}
				}
			} else {
				if (test) {
					setConditionerror("This rule expression does not return true/false");
				} else {
					throw new PropertiesException("This rule expression does not return true/false", null, "A condition must return true/false", condition.getExpression());
				}
			}
			return r;
		} else {
			return null;
		}
	}

	public void setConditionerror(String conditionerror) {
		this.conditionerror = conditionerror;
	}

	public void setSubstituteerror(String substituteerror) {
		this.substituteerror = substituteerror;
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
				setSampleresult(null);
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
		if (substitute != null && substitute.length() > 0) {
			try {
				this.substitute = new PrimitiveMapping(substitute);
			} catch (IOException e) {
				setSampleoutput(null);
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
			return getFieldname() + ": PrimitiveRule: if (" + condition.toString() + ") then PASS else " + iffalse.name();
		} else {
			return getFieldname() + ": PrimitiveRule always PASS ";
		}
	}

	public String getSubstituteerror() {
		return substituteerror;
	}

	public String getConditionerror() {
		return conditionerror;
	}

}
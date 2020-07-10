package io.rtdi.bigdata.rulesservice.rules;

import java.util.List;

import org.apache.avro.Schema;

import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.RuleResult;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.pipeline.foundation.mapping.PrimitiveMapping;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.ValueSchema;

public class PrimitiveRule extends RuleWithName<Rule<?>> {
	private PrimitiveMapping condition;
	private RuleResult iffalse = RuleResult.PASS;
	private PrimitiveMapping substitute;

	public PrimitiveRule(String fieldname, String rulename, String condition, RuleResult iffalse, String substitute) {
		super(fieldname, rulename);
		this.condition = new PrimitiveMapping(condition);
		this.iffalse = iffalse;
		if (substitute != null) {
			this.substitute = new PrimitiveMapping(substitute);
		}
	}
	
	public PrimitiveRule() {
		super();
	}

	@Override
	public RuleResult apply(JexlRecord valuerecord, List<JexlRecord> ruleresults) {
		Object o = condition.evaluate(valuerecord);
		if (o != null && o instanceof Boolean) {
			RuleResult result;
			if (((Boolean) o).booleanValue()) {
				result = RuleResult.PASS;
			} else {
				result = iffalse;
			}

			JexlRecord r = ValueSchema.createAuditDetails();
			r.put(ValueSchema.AUDITTRANSFORMATIONNAME, getRulename());
			r.put(ValueSchema.TRANSFORMRESULT, result.name());
			// r.put(ValueSchema.AUDITTRANSFORMRESULTTEXT, value);
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
			this.condition = new PrimitiveMapping(condition);
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
			this.substitute = new PrimitiveMapping(substitute);
		}
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

}
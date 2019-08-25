package io.rtdi.bigdata.rulesservice.rules;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlArray;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.RuleResult;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

public class ArrayRule extends RecordRule {

	public ArrayRule(String fieldname) {
		super(fieldname);
	}
	
	public ArrayRule() {
		super();
	}

	@Override
	public RuleResult apply(JexlRecord valuerecord, List<JexlRecord> ruleresults) {
		Object o = valuerecord.get(getFieldname());
		if (o != null && o instanceof JexlArray) {
			JexlArray<?> l = (JexlArray<?>) o;
			for ( Object r : l) {
				if (r instanceof JexlRecord) {
					for ( Rule<?> rule : getRules()) {
						rule.apply((JexlRecord) r, ruleresults);
					}
				}
			}
		}
		return null;
	}

	@Override
	public String toString() {
		return "ArrayRule for field \"" + getFieldname() + "\" tests all records";
	}
	
	public static ArrayRule createUIRuleTree(String fieldname, Schema schema, List<Rule<?>> originalrules) throws PropertiesException {
		if (schema.getType() == Type.ARRAY) {
			ArrayRule a = new ArrayRule(fieldname);
			addFields(a, schema.getElementType(), originalrules);
			return a;
		} else {
			throw new PropertiesException("Provided Schema is not a record schema", (String) null, null, schema.getName());
		}
	}


	@Override
	public ArrayRule createUIRuleTree(Schema schema) throws PropertiesException {
		return createUIRuleTree(getFieldname(), schema, getRules());
	}
	
}
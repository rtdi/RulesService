package io.rtdi.bigdata.rulesservice.rules;

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlArray;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.kafka.avro.RuleResult;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

@JsonIgnoreProperties(ignoreUnknown=true)
public class ArrayRule extends RecordRule {

	public ArrayRule(String fieldname) {
		super(fieldname);
	}
	
	public ArrayRule() {
		super();
	}

	@Override
	public RuleResult apply(JexlRecord valuerecord, List<JexlRecord> ruleresults) throws IOException {
		Object o = valuerecord.get(getFieldname());
		if (o != null && o instanceof JexlArray) {
			JexlArray<?> l = (JexlArray<?>) o;
			for ( Object r : l) {
				if (r instanceof JexlRecord) {
					for ( Rule rule : getRules()) {
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
	
	public static ArrayRule createUIRuleTree(String fieldname, Schema schema, List<Rule> originalrules) throws PropertiesException {
		if (schema.getType() == Type.ARRAY) {
			ArrayRule a = new ArrayRule(fieldname);
			addFields(a, schema.getElementType(), originalrules);
			return a;
		} else {
			throw new PropertiesException("Provided Schema is not a record schema", (String) null, schema.getName());
		}
	}


	@Override
	public ArrayRule createUIRuleTree(Schema schema) throws PropertiesException {
		return createUIRuleTree(getFieldname(), schema, getRules());
	}
	
	@Override
	public void assignSamplevalue(JexlRecord sampledata) {
		Object data = sampledata.get(getFieldname());
		if (data instanceof List) {
			List<?> l = (List<?>) data;
			Object s = l.get(0);
			if (s instanceof JexlRecord && this.getRules() != null) {
				JexlRecord samplerecord = (JexlRecord) s;
				for ( Rule r : this.getRules()) {
					r.assignSamplevalue(samplerecord);
				}
			}
		}
	}

	@Override
	public RuleResult validateRule(JexlRecord valuerecord) {
		Object o = valuerecord.get(getFieldname());
		if (o != null && o instanceof JexlArray) {
			JexlArray<?> l = (JexlArray<?>) o;
			Object s = l.get(0);
			if (s instanceof JexlRecord && this.getRules() != null) {
				JexlRecord samplerecord = (JexlRecord) s;
				for ( Rule r : this.getRules()) {
					r.validateRule(samplerecord);
				}
			}
		}
		return null;
	}

	@Override
	protected Rule createNewInstance() throws ConnectorCallerException {
		return new ArrayRule(getFieldname());
	}

}
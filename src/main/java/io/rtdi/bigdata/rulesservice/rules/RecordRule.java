package io.rtdi.bigdata.rulesservice.rules;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.kafka.avro.RuleResult;
import io.rtdi.bigdata.kafka.avro.datatypes.*;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.pipeline.foundation.utils.IOUtils;

@JsonIgnoreProperties(ignoreUnknown=true)
public class RecordRule extends Rule {
	
	public RecordRule() {
		super();
	}
	
	public RecordRule(String fieldname) {
		super(fieldname);
	}

	public ArrayRule addNested(String fieldname) {
		ArrayRule arrayrule = new ArrayRule(fieldname);
		addRule(arrayrule);
		return arrayrule;
	}
	
	@Override
	public RuleResult apply(JexlRecord valuerecord, List<JexlRecord> ruleresults) throws IOException {
		if (getRules() != null) {
			if (getFieldname() != null) {
				Object o = valuerecord.get(getFieldname());
				if (o instanceof JexlRecord) {
					for ( Rule rule : getRules()) {
						rule.apply((JexlRecord) o, ruleresults);
					}
				}
			}
		}
		return null;
	}
		
	@Override
	public String toString() {
		return "RecordRule \"" + getFieldname() + "\" tests this record";
	}

	@Override
	public RecordRule createUIRuleTree(Schema schema) throws PropertiesException {
		return createUIRuleTree(getFieldname(), schema, getRules());
	}
	
	public static RecordRule createUIRuleTree(String fieldname, Schema schema, List<Rule> originalrules) throws PropertiesException {
		if (schema.getType() == Type.RECORD) {
			RecordRule r = new RecordRule(fieldname);
			addFields(r, schema, originalrules);
			return r;
		} else {
			throw new PropertiesException("Provided Schema is not a record schema", (String) null, schema.getName());
		}
	}
	
	protected static void addFields(RecordRule r, Schema schema, List<Rule> originalrules) throws PropertiesException {
		Map<String, Rule> fieldnameindex = new HashMap<>();
		if (originalrules != null) {
			for (Rule o : originalrules) {
				String fieldname = o.getFieldname();
				fieldnameindex.put(fieldname, o);
			}
		}
		/*
		 * The Rule array is built using the schema as basis to preserve the schema's field order 
		 */
		List<Rule> rules = new ArrayList<>();
		if (schema.getType() == Type.RECORD) {
			for (Field field : schema.getFields()) {
				Schema fieldschema = IOUtils.getBaseSchema(field.schema());
				String fieldname = field.name();
				Rule childrule = fieldnameindex.get(fieldname);
				if (childrule == null) {
					switch (fieldschema.getType()) {
					case ARRAY:
						childrule = ArrayRule.createUIRuleTree(fieldname, fieldschema, null);
						break;
					case RECORD:
						childrule = createUIRuleTree(fieldname, fieldschema, null);
						break;
					default:
						childrule = new EmptyRule(fieldname);
						break;
					}
					childrule.setDataType(AvroType.getAvroDataType(fieldschema));
					rules.add(childrule);
				} else {
					Rule c = childrule.createUIRuleTree(fieldschema);
					c.setDataType(AvroType.getAvroDataType(fieldschema));
					rules.add(c);
				}
				
			}
			r.setRules(rules);
		}
	}

	@Override
	public void assignSamplevalue(JexlRecord sampledata) {
		if (sampledata != null && this.getRules() != null) {
			if (getFieldname() != null) {
				Object o = sampledata.get(getFieldname());
				if (o instanceof JexlRecord) {
					for ( Rule r : this.getRules()) {
						r.assignSamplevalue((JexlRecord) o);
					}
				}
			}
		}
	}

	@Override
	public String getSamplevalue() {
		return null;
	}

	@Override
	public RuleResult getSampleresult() throws PipelineCallerException {
		return null;
	}

	@Override
	public RuleResult validateRule(JexlRecord valuerecord) {
		if (valuerecord != null && this.getRules() != null) {
			if (getFieldname() != null) {
				Object o = valuerecord.get(getFieldname());
				if (o instanceof JexlRecord) {
					for ( Rule r : this.getRules()) {
						r.validateRule((JexlRecord) o);
					}
				}
			}
		}
		return null;
	}

	@Override
	protected Rule createNewInstance() throws ConnectorCallerException {
		return new RecordRule(getFieldname());
	}

}
package io.rtdi.bigdata.rulesservice.rules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

import io.rtdi.bigdata.connector.pipeline.foundation.IOUtils;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.RuleResult;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

public class RecordRule extends Rule<Rule<?>> {
	
	public RecordRule() {
		super();
	}
	
	public RecordRule(String fieldname) {
		super(fieldname);
	}

	public void addRule(String fieldname, String ruleset, String rulename, String testname, String condition, RuleResult iffalse, String substitute) {
		addRule(new TestSetFirstPass(fieldname, ruleset, new PrimitiveRule(fieldname, rulename, condition, iffalse, substitute)));
	}

	public ArrayRule addNested(String fieldname) {
		ArrayRule arrayrule = new ArrayRule(fieldname);
		addRule(arrayrule);
		return arrayrule;
	}
	
	@Override
	public RuleResult apply(JexlRecord valuerecord, List<JexlRecord> ruleresults) {
		for ( Rule<?> rule : getRules()) {
			rule.apply(valuerecord, ruleresults);
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
	
	public static RecordRule createUIRuleTree(String fieldname, Schema schema, List<Rule<?>> originalrules) throws PropertiesException {
		if (schema.getType() == Type.RECORD) {
			RecordRule r = new RecordRule(fieldname);
			addFields(r, schema, originalrules);
			return r;
		} else {
			throw new PropertiesException("Provided Schema is not a record schema", (String) null, null, schema.getName());
		}
	}
	
	protected static void addFields(RecordRule r, Schema schema, List<Rule<?>> originalrules) throws PropertiesException {
		Map<String, Rule<?>> fieldnameindex = new HashMap<>();
		if (originalrules != null) {
			for (Rule<?> o : originalrules) {
				String fieldname = o.getFieldname();
				fieldnameindex.put(fieldname, o);
			}
		}
		/*
		 * The Rule array is built using the schema as basis to preserve the schema's field order 
		 */
		List<Rule<?>> rules = new ArrayList<>();
		for (Field field : schema.getFields()) {
			Schema fieldschema = IOUtils.getBaseSchema(field.schema());
			String fieldname = field.name();
			Rule<?> childrule = fieldnameindex.get(fieldname);
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
				rules.add(childrule);
			} else {
				rules.add(childrule.createUIRuleTree(fieldschema));
			}
			
		}
		r.setRules(rules);
	}

}
package io.rtdi.bigdata.rulesservice.rules;

import org.apache.avro.Schema.Field;

import io.rtdi.bigdata.kafka.avro.AvroUtils;

public class RuleUtils {

	/**
	 * Create the default rule tree for a (sub-)schema
	 * @param schema is the input structure
	 * @param name of the rule
	 * @return one rule matching the schema; the rule might have child rules
	 */
	public static Rule getRuleForSchema(org.apache.avro.Schema schema, String name) {
		schema = AvroUtils.getBaseSchema(schema); // remove the optional field, meaning union(null, string) --> string
		RecordRule recordrule;
		switch (schema.getType()) {
		case BOOLEAN:
		case BYTES:
		case DOUBLE:
		case ENUM:
		case FIXED:
		case FLOAT:
		case INT:
		case LONG:
		case MAP:
		case STRING:
			return new EmptyRule(name, schema);
		case UNION:
			/*
			 * Above getBaseSchema() handles UNION of NULL plus something else.
			 * But if the UNION is more complicated, more handling is required
			 */
			return new UnionRule(name, schema);
		case NULL:
			return new EmptyRule(name, schema);
		case RECORD:
			recordrule = new RecordRule(name, schema);
			addRules(recordrule, schema);
			return recordrule;
		case ARRAY:
			ArrayRule arrayrule = new ArrayRule(name, schema);
			arrayrule.addRule(getRuleForSchema(schema.getElementType(), name));
			return arrayrule;
		default:
			return new EmptyRule(name, schema);
		}
	}

	public static void addRules(RecordRule r, org.apache.avro.Schema schema) {
		for (Field f : schema.getFields()) {
			Rule rule = getRuleForSchema(f.schema(), f.name());
			if (rule != null) {
				r.addRule(rule);
			}
		}
		GenericRules gen = new GenericRules();
		gen.addRule(new EmptyRule(null, null));
		r.addRule(gen); // Record rules have rules on fields and unbound generic rules
	}

}

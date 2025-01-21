package io.rtdi.bigdata.rulesservice.rules;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

import io.rtdi.bigdata.kafka.avro.AvroUtils;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroType;
import io.rtdi.bigdata.kafka.avro.datatypes.IAvroDatatype;
import io.rtdi.bigdata.rulesservice.PropertiesException;
import io.rtdi.bigdata.rulesservice.jexl.JexlArray;
import io.rtdi.bigdata.rulesservice.jexl.JexlRecord;

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

	public static Object getSampleValue(Rule rule, Schema schema) throws PropertiesException {
		if (schema != null) {
			switch (schema.getType()) {
			case ARRAY:
				if (rule instanceof ArrayRule ar) {
					if (ar.getRules() != null && ar.getRules().size() > 0) {
						JexlArray<Object> a = new JexlArray<>(schema, null);
						Object value = getSampleValue(rule.getRules().get(0), schema.getElementType());
						if (value != null) {
							a.add(value);
						}
						return a;
					}
				} else {
					throw new PropertiesException("There is a mismatch between the schema and rule type for the field <" + rule.getFieldname() + ">");
				}
				break;
			case RECORD:
				if (rule instanceof RecordRule) {
					if (rule.getRules() != null) {
						JexlRecord rec = new JexlRecord(schema, null);
						for ( Rule r : rule.getRules()) {
							if (r.getFieldname() != null) {
								Field field = schema.getField(r.getFieldname());
								if (field != null) {
									Schema fieldschema = AvroUtils.getBaseSchema(field.schema());
									rec.put(r.getFieldname(), getSampleValue(r, fieldschema));
								}
							}
						}
						return rec;
					}
				} else {
					throw new PropertiesException("There is a mismatch between the schema and rule type for the field <" + rule.getFieldname() + ">");
				}
				break;
			case UNION:
				if (rule instanceof UnionRule) {
					if (rule.getRules() != null) {
						for ( Rule r : rule.getRules()) {
							if (r.getSampleinput() != null) {
								return getSampleValue(r, RuleUtils.findUnionSchema(rule.getSchemaname(), schema.getTypes()));
							}
						}
					}
				}
				break;
			case BOOLEAN:
			case BYTES:
			case DOUBLE:
			case ENUM:
			case FIXED:
			case FLOAT:
			case INT:
			case LONG:
			case MAP:
			case NULL:
			case STRING:
				IAvroDatatype dt = AvroType.getAvroDataType(schema);
				if (dt != null) {
					return dt.convertToInternal(rule.getSampleinput());
				}
				break;
			default:
				break;
			}
		}
		return null;
	}

	public static Schema findUnionSchema(String schemaname, List<Schema> list) {
		for (Schema s : list) {
			if (s.getFullName().equals(schemaname)) {
				return s;
			}
		}
		return null;
	}

}

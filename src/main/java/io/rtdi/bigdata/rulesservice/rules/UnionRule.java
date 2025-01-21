package io.rtdi.bigdata.rulesservice.rules;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

import io.rtdi.bigdata.kafka.avro.RuleResult;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroUnion;
import io.rtdi.bigdata.rulesservice.jexl.AvroContainer;
import io.rtdi.bigdata.rulesservice.jexl.JexlRecord;

public class UnionRule extends Rule implements IContainerRule {

	public UnionRule() {
		super();
	}

	public UnionRule(String name, Schema schema) {
		super(name, schema != null ? schema.getFullName() : null);
		setDataType(AvroUnion.create());
		if (schema != null && schema.getType() == Type.UNION) {
			for (Schema s : schema.getTypes()) {
				Rule r = RuleUtils.getRuleForSchema(s, name);
				if (r != null) {
					addRule(r);
				}
			}
		}
	}

	@Override
	public RuleResult apply(Object value, AvroContainer container, boolean test) throws IOException {
		// List<Schema> unionschema = container.getSchema().getField(getFieldname()).schema().getTypes();
		// TODO: Currently it works on the base schema only, but a union could also encompass multiple types as long as they have different names

		for (Rule r : getRules()) {
			if (r.getDataType() != null) {
				switch (r.getDataType().getBackingType()) {
				case ARRAY:
					if (value instanceof Collection) {
						r.apply(value, container, test);
					}
					break;
				case BOOLEAN:
					if (value instanceof Boolean) {
						r.apply(value, container, test);
					}
					break;
				case BYTES:
					if (value instanceof byte[]) {
						r.apply(value, container, test);
					}
					break;
				case DOUBLE:
					if (value instanceof Double) {
						r.apply(value, container, test);
					}
					break;
				case ENUM:
					if (value instanceof CharSequence) {
						r.apply(value, container, test);
					}
					break;
				case FIXED:
					if (value instanceof CharSequence) {
						r.apply(value, container, test);
					}
					break;
				case FLOAT:
					if (value instanceof Float) {
						r.apply(value, container, test);
					}
					break;
				case INT:
					if (value instanceof Integer) {
						r.apply(value, container, test);
					}
					break;
				case LONG:
					if (value instanceof Long) {
						r.apply(value, container, test);
					}
					break;
				case MAP:
					if (value instanceof Map) {
						r.apply(value, container, test);
					}
					break;
				case NULL:
					if (value == null) {
						r.apply(value, container, test);
					}
					break;
				case RECORD:
					if (value instanceof JexlRecord) {
						r.apply(value, container, test);
					}
					break;
				case STRING:
					if (value instanceof String) {
						r.apply(value, container, test);
					}
					break;
				case UNION:
					// union of union is not allowed according to the Avro specification
					break;
				default:
					break;

				}
			} else {
				return null;
			}
		}
		return null;
	}

	@Override
	public void update(IContainerRule empty) {
		if (empty instanceof UnionRule) {
			if (getRules() != null && empty.getRules() != null) {
				for ( int i=0; i<getRules().size(); i++ ) {
					Rule rule = getRules().get(i);
					if (empty.getRules().size() > i) {
						Rule emptyrule = getRules().get(i);
						if (rule instanceof IContainerRule rr && emptyrule instanceof IContainerRule er) {
							rr.update(er);
						}
					}
				}
			}
		}
	}

	@Override
	public Rule clone() {
		UnionRule ret = new UnionRule();
		ret.setDataType(getDataType());
		ret.setFieldname(getFieldname());
		ret.setSchemaname(getSchemaname());
		if (getRules() != null) {
			List<Rule> a = new ArrayList<>(getRules().size());
			ret.setRules(a);
			for (Rule r : getRules()) {
				a.add(r.clone());
			}
		}
		return ret;
	}

}

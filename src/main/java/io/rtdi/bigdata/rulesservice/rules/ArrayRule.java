package io.rtdi.bigdata.rulesservice.rules;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.rtdi.bigdata.kafka.avro.RuleResult;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroArray;
import io.rtdi.bigdata.rulesservice.jexl.AvroContainer;
import io.rtdi.bigdata.rulesservice.jexl.JexlArray;
import io.rtdi.bigdata.rulesservice.jexl.JexlRecord;

@JsonIgnoreProperties(ignoreUnknown=true)
public class ArrayRule extends Rule implements IContainerRule {

	public ArrayRule(String fieldname, Schema schema) {
		super(fieldname, schema != null ? schema.getFullName() : null);
		setDataType(AvroArray.create());
	}

	public ArrayRule() {
		super();
	}

	@Override
	public RuleResult apply(Object value, AvroContainer container, boolean test) throws IOException {
		if (value != null && value instanceof JexlArray l) {
			for ( Object r : l) {
				if (getRules() != null) {
					for ( Rule rule : getRules()) {
						if (r instanceof JexlRecord rec) {
							/*
							 * For an array of records, the container is the record itself
							 */
							rule.apply(rec, rec, test);
						} else {
							rule.apply(r, container, test);
						}
					}
				}
			}
		}
		return null;
	}

	@Override
	public String toString() {
		return getFieldname() + ": ArrayRule";
	}

	@Override
	public void update(IContainerRule empty) {
		if (empty instanceof ArrayRule && getRules() != null && getRules().size() > 0) {
			Rule rule = getRules().get(0);
			if (empty instanceof ArrayRule && empty.getRules() != null && empty.getRules().size() > 0) {
				Rule er = empty.getRules().get(0);
				if (rule instanceof IContainerRule rr && er instanceof IContainerRule cr) {
					rr.update(cr);
				}
			}
		}
	}

	@Override
	public Rule clone() {
		ArrayRule ret = new ArrayRule();
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
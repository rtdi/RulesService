package io.rtdi.bigdata.rulesservice.rules;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.avro.Schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.rtdi.bigdata.kafka.avro.RuleResult;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroRecord;
import io.rtdi.bigdata.rulesservice.jexl.AvroContainer;
import io.rtdi.bigdata.rulesservice.jexl.JexlRecord;

@JsonIgnoreProperties(ignoreUnknown=true)
public class RecordRule extends Rule implements IContainerRule {

	public RecordRule() {
		super();
	}

	public RecordRule(String fieldname, Schema schema) {
		super(fieldname, schema != null ? schema.getFullName() : null);
		setDataType(AvroRecord.create());
	}

	@Override
	public RuleResult apply(Object value, AvroContainer container, boolean test) throws IOException {
		if (getRules() != null) {
			if (value instanceof JexlRecord rec) {
				for ( Rule rule : getRules()) {
					if (rule.getFieldname() != null && !(rule instanceof GenericRules)) {
						rule.apply(rec.get(rule.getFieldname()), container, test);
					} else {
						rule.apply(rec, rec, test);
					}
				}
			}
		}
		return null;
	}

	@Override
	public String toString() {
		return getFieldname() + ": RecordRule tests all fields of this record";
	}

	@Override
	public RuleResult getSampleresult() {
		return null;
	}

	@Override
	public void update(IContainerRule empty) {
		if (empty instanceof RecordRule && empty.getRules() != null) {
			Map<String, Rule> index = getRules().stream().collect(Collectors.toMap(Rule::getFieldname, Function.identity()));
			for (Rule rule : empty.getRules()) {
				Rule r = index.get(rule.getFieldname());
				if (r == null) {
					addRule(rule.clone());
				} else if (r instanceof IContainerRule rr) {
					if (rule instanceof IContainerRule cr) {
						rr.update(cr);
					}
				}
			}
		}
	}

	@Override
	public Rule clone() {
		RecordRule ret = new RecordRule();
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
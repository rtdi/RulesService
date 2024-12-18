package io.rtdi.bigdata.rulesservice.rules;

import java.io.IOException;

import org.apache.avro.Schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.rtdi.bigdata.connector.pipeline.foundation.avro.AvroJexlContext;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlRecord;
import io.rtdi.bigdata.kafka.avro.RuleResult;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroRecord;

@JsonIgnoreProperties(ignoreUnknown=true)
public class RecordRule extends Rule {

	public RecordRule() {
		super();
	}

	public RecordRule(String fieldname, Schema schema) {
		super(fieldname, schema != null ? schema.getFullName() : null);
		setDataType(AvroRecord.create());
	}

	@Override
	public RuleResult apply(Object value, AvroJexlContext container, boolean test) throws IOException {
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

}
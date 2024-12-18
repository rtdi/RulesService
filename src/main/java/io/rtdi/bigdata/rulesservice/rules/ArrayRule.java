package io.rtdi.bigdata.rulesservice.rules;

import java.io.IOException;

import org.apache.avro.Schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.rtdi.bigdata.connector.pipeline.foundation.avro.AvroJexlContext;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlArray;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlRecord;
import io.rtdi.bigdata.kafka.avro.RuleResult;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroArray;

@JsonIgnoreProperties(ignoreUnknown=true)
public class ArrayRule extends Rule {

	public ArrayRule(String fieldname, Schema schema) {
		super(fieldname, schema != null ? schema.getFullName() : null);
		setDataType(AvroArray.create());
	}

	public ArrayRule() {
		super();
	}

	@Override
	public RuleResult apply(Object value, AvroJexlContext container, boolean test) throws IOException {
		if (value != null && value instanceof JexlArray l) {
			for ( Object r : l) {
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
		return null;
	}

	@Override
	public String toString() {
		return getFieldname() + ": ArrayRule";
	}

}
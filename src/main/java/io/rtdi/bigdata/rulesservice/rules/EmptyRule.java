package io.rtdi.bigdata.rulesservice.rules;

import org.apache.avro.Schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.rtdi.bigdata.kafka.avro.RuleResult;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroType;
import io.rtdi.bigdata.kafka.avro.datatypes.IAvroDatatype;
import io.rtdi.bigdata.rulesservice.jexl.AvroContainer;

@JsonIgnoreProperties(ignoreUnknown=true)
public class EmptyRule extends Rule {

	public EmptyRule() {
		super();
	}

	public EmptyRule(String fieldname, Schema schema) {
		super(fieldname, schema != null ? schema.getFullName() : null);
		if (schema != null) {
			IAvroDatatype dt = AvroType.getAvroDataType(schema);
			this.setDataType(dt);
		}
	}

	@Override
	public RuleResult apply(Object value, AvroContainer container, boolean test) {
		setSampleValue(value, test);
		return null;
	}

	@Override
	public RuleResult getSampleresult() {
		return null;
	}

	@Override
	public String toString() {
		return getFieldname() + ": EmptyRule";
	}

	@Override
	public Rule clone() {
		EmptyRule ret = new EmptyRule();
		ret.setDataType(getDataType());
		ret.setFieldname(getFieldname());
		ret.setSchemaname(getSchemaname());
		return ret;
	}

}

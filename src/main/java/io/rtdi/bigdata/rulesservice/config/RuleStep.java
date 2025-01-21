package io.rtdi.bigdata.rulesservice.config;

import java.io.IOException;

import org.apache.avro.Schema;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.rtdi.bigdata.kafka.avro.RuleResult;
import io.rtdi.bigdata.rulesservice.jexl.AvroContainer;
import io.rtdi.bigdata.rulesservice.jexl.JexlRecord;
import io.rtdi.bigdata.rulesservice.rules.GenericRules;
import io.rtdi.bigdata.rulesservice.rules.RecordRule;
import io.rtdi.bigdata.rulesservice.rules.Rule;

public class RuleStep extends RecordRule {
	private static ObjectMapper mapper;

	static {
		mapper = new ObjectMapper();
		mapper.setSerializationInclusion(Include.NON_NULL);
	}

	public RuleStep(String name, Schema schema) {
		super(name, schema);
	}

	public RuleStep() {
	}

	@Override
	public RuleResult apply(Object value, AvroContainer container, boolean test) throws IOException {
		if (getRules() != null) {
			for ( Rule rule : getRules()) {
				if (rule.getFieldname() != null && !(rule instanceof GenericRules)) {
					rule.apply(container.get(rule.getFieldname()), container, test);
				} else {
					/*
					 * GenericRules are not bound to a field, they operate on the entire record
					 */
					rule.apply(container, container, test);
				}
			}
		}
		return null;
	}

	@Override
	public String toString() {
		return getFieldname() + ": RuleStep";
	}

	@Override
	public void updateSampleOutput(JexlRecord valuerecord) {
		if (getRules() != null) {
			for ( Rule rule : getRules()) {
				rule.updateSampleOutput(valuerecord);
			}
		}
	}
}

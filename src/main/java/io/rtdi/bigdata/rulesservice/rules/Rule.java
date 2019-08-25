package io.rtdi.bigdata.rulesservice.rules;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.avro.Schema;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;

import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.RuleResult;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

@JsonTypeInfo(
		  use = JsonTypeInfo.Id.NAME, 
		  include = JsonTypeInfo.As.PROPERTY, 
		  property = "type")
@JsonSubTypes({ 
	@Type(value = EmptyRule.class, name = "EmptyRule"), 
	@Type(value = ArrayRule.class, name = "ArrayRule"), 
	@Type(value = PrimitiveRule.class, name = "PrimitiveRule"), 
	@Type(value = TestSetAll.class, name = "TestSetAll"), 
	@Type(value = TestSetFirstPass.class, name = "TestSetFirstPass"), 
	@Type(value = TestSetFirstFail.class, name = "TestSetFirstFail"), 
	@Type(value = RecordRule.class, name = "RecordRule") 
})
public abstract class Rule<R extends Rule<?>> {
	private String fieldname;
	private List<R> rules = null; 
	
	public Rule() {
		super();
	}

	public Rule(String fieldname) {
		this();
		this.fieldname = fieldname;
	}

	public abstract RuleResult apply(JexlRecord valuerecord, List<JexlRecord> ruleresults);

	public List<R> getRules() {
		return rules;
	}

	public void setRules(List<R> rules) {
		this.rules = rules;
	}
	
	public void addAll(Collection<R> m) {
		if (rules == null) {
			rules = new ArrayList<>();
		}
		rules.addAll(m);
	}

	protected void addRule(R r) {
		if (rules == null) {
			rules = new ArrayList<>();
		}
		rules.add(r);
	}

	public String getFieldname() {
		return fieldname;
	}

	public void setFieldname(String fieldname) {
		this.fieldname = fieldname;
	}

	protected abstract Rule<?> createUIRuleTree(Schema fieldschema) throws PropertiesException;
	
}
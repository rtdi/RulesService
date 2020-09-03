package io.rtdi.bigdata.rulesservice.rules;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroType;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.IAvroDatatype;
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
@JsonIgnoreProperties(ignoreUnknown=true)
public abstract class Rule {
	private String fieldname;
	private List<Rule> rules = null;
	private IAvroDatatype fielddatatype;
	protected String rulepath;
	private String sampleresulterror = null;
	
	public Rule() {
		super();
	}

	public Rule(String fieldname) {
		this();
		this.fieldname = fieldname;
	}

	public abstract RuleResult apply(JexlRecord valuerecord, List<JexlRecord> ruleresults) throws IOException;

	public List<Rule> getRules() {
		return rules;
	}

	public void postSerialization() {
		if (rules != null) {
			for (Rule r : rules) {
				r.setParentPath(rulepath);
				r.postSerialization();
			}
		}
	}
	
	public void setRules(List<Rule> rules) {
		this.rules = rules;
	}
	
	public void addAll(Collection<Rule> m) {
		if (rules == null) {
			rules = new ArrayList<>();
		}
		Iterator<Rule> iter = m.iterator();
		while (iter.hasNext()) {
			addRule(iter.next());
		}
	}
	
	protected String getRulePath() {
		return rulepath;
	}

	public void addRule(Rule r) {
		if (rules == null) {
			rules = new ArrayList<>();
		}
		rules.add(r);
		r.setParentPath(rulepath);
	}

	public String getFieldname() {
		return fieldname;
	}

	public void setFieldname(String fieldname) {
		this.fieldname = fieldname;
	}

	protected abstract Rule createUIRuleTree(Schema fieldschema) throws PropertiesException;

	public String getFielddatatype() {
		if (fielddatatype != null) {
			return fielddatatype.toString();
		} else {
			return null;
		}
	}

	public void setFielddatatype(String fielddatatype) {
		this.fielddatatype = AvroType.getDataTypeFromString(fielddatatype);
	}

	protected void setDataType(IAvroDatatype fielddatatype) {
		this.fielddatatype = fielddatatype;
	}
	
	protected IAvroDatatype getDataType() {
		return fielddatatype;
	}

	public void setParentPath(String parentpath) {
		rulepath = parentpath;
	}

	public abstract void assignSamplevalue(JexlRecord record);

	public abstract Object getSamplevalue() throws IOException;

	public abstract RuleResult getSampleresult() throws IOException;

	public abstract RuleResult validateRule(JexlRecord valuerecord);

	public String getSampleresulterror() {
		return sampleresulterror;
	}

	protected Rule createSimplified() throws IOException {
		Rule n = createNewInstance();
		addAllMandatory(n);
		if (n.getRules() == null || n.getRules().size() == 0) {
			return null;
		} else {
			return n;
		}
	}

	protected abstract Rule createNewInstance() throws IOException;

	protected void addAllMandatory(Rule target) throws IOException {
		if (getRules() != null) {
			for (Rule r : getRules()) {
				Rule n = r.createSimplified();
				if (n != null) {
					target.addRule(n);
				}
			}
		}
	}

}
package io.rtdi.bigdata.rulesservice.rules;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import io.rtdi.bigdata.connector.pipeline.foundation.avro.AvroJexlContext;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlRecord;
import io.rtdi.bigdata.kafka.avro.AvroUtils;
import io.rtdi.bigdata.kafka.avro.RuleResult;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroType;
import io.rtdi.bigdata.kafka.avro.datatypes.IAvroDatatype;

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
	@Type(value = RecordRule.class, name = "RecordRule"),
	@Type(value = UnionRule.class, name = "UnionRule"),
	@Type(value = GenericRules.class, name = "GenericRules")
})
@JsonIgnoreProperties(ignoreUnknown=true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class Rule {
	private String fieldname;
	private List<Rule> rules = null;
	private IAvroDatatype fielddatatype;
	protected String rulepath;
	private Object sampleinput = null;
	private Object sampleoutput = null;
	private RuleResult sampleresult;
	private String schemaname;

	public Rule() {
		super();
	}

	public Rule(String fieldname, String schemaname) {
		this();
		this.fieldname = fieldname;
		this.schemaname = schemaname;
	}

	protected void setSampleValue(Object value, boolean test) {
		if (test) {
			setSampleinput(Rule.valueToJavaObject(value, getDataType()));
			setSampleoutput(getSampleinput()); // Might be overwritten later
		}
	}

	public abstract RuleResult apply(Object value, AvroJexlContext container, boolean test) throws IOException;

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

	public void setSampleoutput(String sampleoutput) {
		this.sampleoutput = sampleoutput;
	}

	public Object getSampleoutput() {
		return sampleoutput;
	}

	public void setSampleoutput(Object sampleoutput) {
		if (sampleoutput instanceof GenericRecord) {
			// ignore
		} else if (sampleoutput instanceof List) {
			// ignore
		} else {
			this.sampleoutput = sampleoutput;
		}
	}
	public Object getSampleinput() {
		return sampleinput;
	}

	public void setSampleinput(Object sampleinput) {
		this.sampleinput = sampleinput;
	}

	public static Object valueToJavaObject(Object value, IAvroDatatype datatype) {
		if (value != null && datatype != null) {
			return datatype.convertToJava(value);
		} else {
			return null;
		}
	}

	public RuleResult getSampleresult() {
		return sampleresult;
	}

	public void setSampleresult(RuleResult sampleresult) {
		this.sampleresult = sampleresult;
	}

	/**
	 * Add the substitution value to the changedvalues map
	 *
	 * @param valuerecord
	 * @param fieldname
	 * @param substitutevalue
	 * @param changedvalues
	 */
	public static void addChangedvalue(JexlRecord valuerecord, String fieldname, Object substitutevalue, Map<JexlRecord, Map<String, Object>> changedvalues) {
		Map<String, Object> fieldmap = changedvalues.get(valuerecord);
		if (fieldmap == null) {
			fieldmap = new HashMap<>();
			changedvalues.put(valuerecord, fieldmap);
		}
		fieldmap.put(fieldname, substitutevalue);
	}

	public void updateSampleOutput(JexlRecord valuerecord) {
		Object value = valuerecord.get(fieldname);
		if (value instanceof List l) {
			if (l.size() > 0) {
				Schema c = AvroUtils.getBaseSchema(valuerecord.getSchema().getField(fieldname).schema());
				setSampleoutput(valueToJavaObject(l.get(0), AvroType.getDataType(AvroType.getType(c.getElementType()), 10, 0)));
			}
		}
		setSampleoutput(valueToJavaObject(value, getDataType()));
	}

	public String getSchemaname() {
		return schemaname;
	}

	public void setSchemaname(String schemaname) {
		this.schemaname = schemaname;
	}
}
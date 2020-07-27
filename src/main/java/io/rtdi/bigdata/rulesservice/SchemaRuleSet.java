package io.rtdi.bigdata.rulesservice;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.RowType;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.ValueSchema;
import io.rtdi.bigdata.rulesservice.rules.RecordRule;
import io.rtdi.bigdata.rulesservice.rules.Rule;

public class SchemaRuleSet extends RecordRule {
	private static ObjectMapper mapper;
	
	static {
		mapper = new ObjectMapper();
		mapper.setSerializationInclusion(Include.NON_NULL);
	}

	public SchemaRuleSet(File schemadir) throws PropertiesException {
		this();
		read(schemadir);
	}
	
	public SchemaRuleSet() {
		super();
	}

	public JexlRecord apply(JexlRecord valuerecord) throws IOException {
		RowType changetype = ValueSchema.getChangeType(valuerecord);
		// Rules are applied to new or changed records but not deleted rows
		if (changetype == null || (changetype != RowType.EXTERMINATE && changetype != RowType.DELETE && changetype != RowType.TRUNCATE)) {
			List<JexlRecord> ruleresults = new ArrayList<>();
			apply(valuerecord, ruleresults);
			ValueSchema.mergeResults(valuerecord, ruleresults);
		}
		return valuerecord;
	}
	
	@Override
	public SchemaRuleSet createUIRuleTree(Schema schema) throws PropertiesException {
		return createUIRuleTree(getFieldname(), schema, getRules());
	}
	
	public void updateSchema(Schema schema) throws PropertiesException {
		addFields(this, schema, getRules());
	}

	public static SchemaRuleSet createUIRuleTree(String fieldname, Schema schema, List<Rule> originalrules) throws PropertiesException {
		if (schema.getType() == Type.RECORD) {
			SchemaRuleSet r = new SchemaRuleSet();
			addFields(r, schema, originalrules);
			return r;
		} else {
			throw new PropertiesException("Provided Schema is not a record schema", (String) null, schema.getName());
		}
	}


	private void read(File directory) throws PropertiesException {
		if (!directory.exists()) {
			throw new PropertiesException("Directory for the rule schema files does not exist", "Use the UI or create the file manually", directory.getAbsolutePath());
		} else if (!directory.isDirectory()) {
			throw new PropertiesException("Specified location exists and is no directory", (String) null, directory.getAbsolutePath());
		} else { 
			File file = new File(directory, "Rule.json");
			if (!file.exists()) {
				// Do nothing if directory exists but file does not
			} else if (!file.canRead()) {
				throw new PropertiesException("Rules file is not read-able", "Check file permissions and users", file.getAbsolutePath());
			} else {
				try {
				    RecordRule pg = mapper.readValue(file, SchemaRuleSet.class);
				    pg.postSerialization();
			        addAll(pg.getRules());
				} catch (PropertiesException e) {
					throw e; // to avoid nesting the exception
				} catch (IOException e) {
					throw new PropertiesException("Cannot parse the json file with the schema rules", e, "check filename and format", file.getName());
				}
			}
		}
	}

	public void write(File directory) throws PropertiesException {
		if (!directory.exists()) {
			throw new PropertiesException("Directory for the schema rule files does not exist", "Use the UI or create the file manually", directory.getAbsolutePath());
		} else if (!directory.isDirectory()) {
			throw new PropertiesException("Specified location exists and is no directory", (String) null, directory.getAbsolutePath());
		} else {
			File file = new File(directory, "Rule.json");
			if (file.exists() && !file.canWrite()) { // Either the file does not exist or it exists and is write-able
				throw new PropertiesException("Schema rule file is not write-able", "Check file permissions and users", file.getAbsolutePath());
			} else {
				try {
	    			mapper.writeValue(file, this.createSimplified());
				} catch (IOException e) {
					throw new PropertiesException("Failed to write the json schema rule file", e, "check filename", file.getName());
				}
				
			}
		}
	}

	@Override
	protected Rule createNewInstance() throws ConnectorCallerException {
		return new SchemaRuleSet();
	}

	@Override
	public String toString() {
		return "Rules for schema";
	}

}

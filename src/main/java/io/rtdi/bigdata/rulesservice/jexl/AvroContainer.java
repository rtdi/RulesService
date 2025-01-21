package io.rtdi.bigdata.rulesservice.jexl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.commons.jexl3.JexlContext;

public interface AvroContainer extends JexlContext {

	/**
	 * Move the collected change values into the record.
	 * After that call the changed value list is empty.
	 */
	void mergeReplacementValues();

	void addChangedvalue(String fieldname, Object substitutevalue);

	String getPath();

	AvroContainer getParent();

	void setParent(AvroContainer parent);

	Field getParentField();

	List<JexlRecord> getRuleresults();

	void setRuleresults(ArrayList<JexlRecord> list) throws IOException;

	void addRuleresult(JexlRecord r) throws IOException;

	Schema getSchema();

	Map<String, Object> toMap();

}

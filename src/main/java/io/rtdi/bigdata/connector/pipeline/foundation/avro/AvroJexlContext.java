package io.rtdi.bigdata.connector.pipeline.foundation.avro;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.commons.jexl3.JexlContext;

public interface AvroJexlContext extends JexlContext {

	/**
	 * Move the collected change values into the record.
	 * After that call the changed value list is empty.
	 */
	void mergeReplacementValues();

	void addChangedvalue(String fieldname, Object substitutevalue);

	String getPath();

	AvroJexlContext getParent();

	void setParent(AvroJexlContext parent);

	Field getParentField();

	List<JexlRecord> getRuleresults();

	void setRuleresults(ArrayList<JexlRecord> list) throws IOException;

	void addRuleresult(JexlRecord r) throws IOException;

	Schema getSchema();

}

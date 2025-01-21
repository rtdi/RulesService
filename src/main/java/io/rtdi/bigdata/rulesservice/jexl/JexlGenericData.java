package io.rtdi.bigdata.rulesservice.jexl;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

public class JexlGenericData extends GenericData {
	private static JexlGenericData INSTANCE = new JexlGenericData();

	public JexlGenericData() {
		super();
	}

	public JexlGenericData(ClassLoader classLoader) {
		super(classLoader);
	}

	public static GenericData get() { return INSTANCE; }

	@Override
	public Object newRecord(Object old, Schema schema) {
		if (old instanceof JexlRecord) {
			JexlRecord record = (JexlRecord) old;
			if (record.getSchema() == schema) {
				return record;
			}
		}
		return new JexlRecord(schema, null);
	}


}

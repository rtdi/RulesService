package io.rtdi.bigdata.rulesservice.jexl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Record;
import org.apache.commons.jexl3.annotations.NoJexl;

import io.rtdi.bigdata.kafka.avro.AvroUtils;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroRecord;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroType;
import io.rtdi.bigdata.kafka.avro.datatypes.IAvroDatatype;

public class JexlRecord extends Record implements AvroContainer {
	private AvroContainer parent = null;
	private int schemaid;
	private Field parentfield;
	int parentarrayindex = -1;
	private String path = null;
	private Map<String, Object> changedvalues = null;
	private List<JexlRecord> ruleresults = null;
	private AvroContainer root = null;
	private Map<String, Object> map = null;

	@NoJexl
	public JexlRecord(Record other, boolean deepCopy) {
		super(other, deepCopy);
	}

	@NoJexl
	public JexlRecord(Schema schema, JexlRecord parent) {
		super(schema);
		setParent(parent);
	}

	@NoJexl
	public JexlRecord(Schema schema, int schemaid) {
		super(schema);
		this.schemaid = schemaid;
		ruleresults = new ArrayList<>();
	}

	@Override
	@NoJexl
	public void setParent(AvroContainer parent) {
		this.parent = parent;
	}

	public AvroContainer getRoot() {
		if (root != null) {
			return root;
		} else {
			AvroContainer root = this;
			while (root.getParent() != null) {
				root = root.getParent();
			}
			return root;
		}
	}

	@Override
	public List<JexlRecord> getRuleresults() {
		return ruleresults;
	}

	@Override
	@NoJexl
	public void mergeReplacementValues() {
		if (changedvalues != null) {
			for( Entry<String, Object> entry : changedvalues.entrySet()) {
				this.put(entry.getKey(), entry.getValue());
			}
			changedvalues.clear();
		}
		for (Field f : getSchema().getFields()) {
			Type t = AvroUtils.getBaseSchema(f.schema()).getType();
			if (t == Type.RECORD || t == Type.ARRAY) {
				Object value = this.get(f.name());
				if (value instanceof AvroContainer r) {
					r.mergeReplacementValues();
				}
			}
		}
	}

	@Override
	public AvroContainer getParent() {
		return parent;
	}

	@Override
	@NoJexl
	public void put(String key, Object value) {
		Field field = getSchema().getField(key);
		if (field == null) {
			throw new AvroRuntimeException("Not a valid schema field: " + key);
		}
		if (value instanceof JexlRecord r) {
			r.setParent(this);
			r.parentfield = field;
		} else if (value instanceof JexlArray<?> a) {
			a.setParent(this);
			a.parentfield = field;
		} else {
			IAvroDatatype t = AvroType.getAvroDataType(field.schema());
			if (t != null) {
				value = t.convertToInternal(value);
			} else {
				throw new AvroRuntimeException("Not a valid schema for field: " + field.name());
			}
		}
		super.put(field.pos(), value);
	}

	@Override
	@NoJexl
	public void put(int i, Object v) {
		Field field = getSchema().getFields().get(i);
		if (v instanceof JexlRecord r) {
			r.setParent(this);
			r.parentfield = field;
		} else if (v instanceof JexlArray<?> a) {
			a.setParent(this);
			a.parentfield = field;
		} else {
			IAvroDatatype t = AvroType.getAvroDataType(field.schema());
			if (t != null) {
				v = t.convertToInternal(v);
			} else {
				throw new AvroRuntimeException("Not a valid schema for field: " + field.name());
			}
		}
		super.put(i, v);
	}

	@NoJexl
	public JexlRecord addChild(String key) {
		Object f = super.get(key);
		if (f == null) {
			Field field = this.getSchema().getField(key);
			if (field != null) {
				Schema s = AvroUtils.getBaseSchema(field.schema());
				if (s.getType() == Type.ARRAY) {
					JexlArray<JexlRecord> a = new JexlArray<>(50, s, this);
					put(key, a);
					JexlRecord r = new JexlRecord(s.getElementType(), this);
					a.add(r);
					return r;
				} else if (s.getType() == Type.RECORD) {
					JexlRecord r = new JexlRecord(s, this);
					put(key, r);
					return r;
				} else {
					throw new AvroRuntimeException("Column \"" + key + "\" is not a sub record or array of records");
				}
			} else {
				throw new AvroRuntimeException("Field \"" + key + "\" does not exist in schema");
			}
		} else if (f instanceof JexlArray) {
			@SuppressWarnings("unchecked")
			JexlArray<JexlRecord> a = (JexlArray<JexlRecord>) f;
			JexlRecord r = new JexlRecord(a.getSchema().getElementType(), this);
			a.add(r);
			return r;
		} else if (f instanceof JexlRecord) {
			throw new AvroRuntimeException("Field \"" + key + "\" is a record and was added already");
		} else {
			throw new AvroRuntimeException("Field \"" + key + "\" is not a sub record or array of records");
		}

	}

	@Override
	public String toString() {
		StringBuffer b = new StringBuffer();
		AvroRecord.create().toString(b, this);
		return b.toString();
	}

	@Override
	public Object get(String key) {
		if ("parent".equals(key)) {
			return parent;
		} else if (getSchema().getField(key) != null) {
			return super.get(key);
		} else {
			return null;
		}
	}

	public int getSchemaId() {
		return schemaid;
	}

	@NoJexl
	public void setSchemaId(int schemaid) {
		this.schemaid = schemaid;
	}

	@Override
	public String getPath() {
		if (path == null) {
			if (parent == null) {
				path = null;
			} else if (parentfield != null) {
				String p = parent.getPath();
				if (p != null) {
					path = p + "." + parentfield.name();
				} else {
					path = parentfield.name();
				}
			} else {
				path = parent.getPath() + "[" + parentarrayindex + "]";
			}
		}
		return path;
	}

	@Override
	public Field getParentField() {
		return parentfield;
	}

	public Map<String, Object> getChangedvalues() {
		return changedvalues;
	}

	@Override
	public void addChangedvalue(String fieldname, Object value) {
		if (changedvalues == null) {
			changedvalues = new HashMap<>();
		}
		changedvalues.put(fieldname, value);
	}

	@Override
	@NoJexl
	public void addRuleresult(JexlRecord r) throws IOException {
		AvroContainer root = getRoot();
		if (root.getRuleresults() == null) {
			root.setRuleresults(new ArrayList<>());
		}
		root.getRuleresults().add(r);
	}

	@Override
	@NoJexl
	public void setRuleresults(ArrayList<JexlRecord> list) throws IOException {
		AvroContainer root = getRoot();
		if (root == this) {
			ruleresults = list;
		} else {
			throw new IOException("rule results can only be set at the root level");
		}
	}

	@Override
	public Map<String, Object> toMap() {
		if (map == null) {
			map = new HashMap<>();
			for (Field f : super.getSchema().getFields()) {
				Object value = super.get(f.name());
				if (value instanceof AvroContainer c) {
					map.put(f.name(), c.toMap());
				} else {
					map.put(f.name(), value);
				}
			}
			AvroContainer p = this.getParent();
			if (p != null) {
				map.put("parent", p.toMap());
			}
		}
		return map;
	}

	@Override
	public boolean has(String key) {
		if ("parent".equals(key)) {
			return true;
		} else {
			return getSchema().getField(key) != null;
		}
	}

	@Override
	@NoJexl
	public void set(String name, Object value) {
		throw new IllegalAccessError("A record is a read-only object");
	}

}
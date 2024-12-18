package io.rtdi.bigdata.connector.pipeline.foundation.avro;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData.Array;

public class JexlArray<T> extends Array<T> implements AvroJexlContext {

	private AvroJexlContext parent = null;
	Field parentfield;
	private int parentarrayindex = -1;
	private String path;
	private Map<String, Object> changedvalues = null;
	private List<JexlRecord> ruleresults = null;
	private AvroJexlContext root = null;

	public JexlArray(Schema schema, Collection<T> c) {
		super(schema, c);
	}

	public JexlArray(int capacity, Schema schema, JexlRecord parent) {
		super(capacity, schema);
		setParent(parent);
	}

	@Override
	public void setParent(AvroJexlContext parent) {
		this.parent = parent;
	}

	@Override
	public void mergeReplacementValues() {
		if (changedvalues != null) {
			for( Entry<String, Object> entry : changedvalues.entrySet()) {
				Object value = entry.getValue();
				this.set(entry.getKey(), value);
			}
			changedvalues.clear();
		}
		if (!this.isEmpty()) {
			Iterator<T> iter = this.iterator();
			while (iter.hasNext()) {
				T c = iter.next();
				if (c instanceof AvroJexlContext r) {
					r.mergeReplacementValues();
				}
			}
		}
	}

	@Override
	public List<JexlRecord> getRuleresults() {
		return ruleresults;
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
	public AvroJexlContext getParent() {
		return parent;
	}

	@Override
	public boolean add(T o) {
		if (o instanceof JexlRecord r) {
			r.setParent(this);
			r.parentarrayindex = super.size();
		}
		return super.add(o);
	}

	@Override
	public void add(int location, T o) {
		if (o instanceof JexlRecord r) {
			r.setParent(this);
			r.parentarrayindex = location;
		}
		super.add(location, o);
	}

	@Override
	public T set(int i, T o) {
		if (o instanceof JexlRecord r) {
			r.setParent(this);
			r.parentarrayindex = i;
		}
		return super.set(i, o);
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

	@Override
	public Object get(String name) {
		try {
			int index = Integer.parseInt(name);
			if (index > 0 && index < this.size()) {
				return this.get(index);
			} else {
				return null;
			}
		} catch (NumberFormatException e) {
			return null;
		}
	}

	@Override
	public boolean has(String name) {
		try {
			int index = Integer.parseInt(name);
			if (index > 0 && index < this.size()) {
				return true;
			} else {
				return false;
			}
		} catch (NumberFormatException e) {
			return false;
		}
	}

	@Override
	public void set(String name, Object value) {
		try {
			@SuppressWarnings("unchecked")
			T v = (T) value;
			if (name.equals("NEW")) {
				this.add(v);
			} else {
				int index = Integer.parseInt(name);
				if (index > 0 && index < this.size()) {
					this.set(index, v);
				} else {
					// do nothing
				}
			}
		} catch (NumberFormatException | ClassCastException e) {
			// ignore
		}
	}

	@Override
	public void addRuleresult(JexlRecord r) throws IOException {
		AvroJexlContext root = getRoot();
		if (root.getRuleresults() == null) {
			root.setRuleresults(new ArrayList<>());
		}
		root.getRuleresults().add(r);
	}

	@Override
	public void setRuleresults(ArrayList<JexlRecord> list) throws IOException {
		AvroJexlContext root = getRoot();
		if (root == this) {
			ruleresults = list;
		} else {
			throw new IOException("rule results can only be set at the root level");
		}
	}

	public AvroJexlContext getRoot() {
		if (root != null) {
			return root;
		} else {
			AvroJexlContext root = this;
			while (root.getParent() != null) {
				root = root.getParent();
			}
			return root;
		}
	}

}
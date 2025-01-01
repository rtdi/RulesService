package io.rtdi.bigdata.rulesservice.jexl;

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
import org.apache.commons.jexl3.annotations.NoJexl;

public class JexlArray<T> extends Array<T> implements AvroContainer {

	private AvroContainer parent = null;
	Field parentfield;
	private int parentarrayindex = -1;
	private String path;
	private Map<String, Object> changedvalues = null;
	private List<JexlRecord> ruleresults = null;
	private AvroContainer root = null;
	private Map<String, Object> map = null;


	@NoJexl
	public JexlArray(Schema schema, Collection<T> c) {
		super(schema, c);
	}

	@NoJexl
	public JexlArray(int capacity, Schema schema, JexlRecord parent) {
		super(capacity, schema);
		setParent(parent);
	}

	@Override
	@NoJexl
	public void setParent(AvroContainer parent) {
		this.parent = parent;
	}

	@Override
	@NoJexl
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
				if (c instanceof AvroContainer r) {
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
	public AvroContainer getParent() {
		return parent;
	}

	@Override
	@NoJexl
	public boolean add(T o) {
		if (o instanceof JexlRecord r) {
			r.setParent(this);
			r.parentarrayindex = super.size();
		}
		return super.add(o);
	}

	@Override
	@NoJexl
	public void add(int location, T o) {
		if (o instanceof JexlRecord r) {
			r.setParent(this);
			r.parentarrayindex = location;
		}
		super.add(location, o);
	}

	@Override
	@NoJexl
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
		if ("parent".equals(name)) {
			return parent;
		} else {
			try {
				int index = Integer.parseInt(name);
				if (index > 0 && index < this.size()) {
					return this.get(index);
				}
			} catch (NumberFormatException e) {
			}
		}
		return null;
	}

	@Override
	@NoJexl
	public void set(String name, Object value) {
		throw new IllegalAccessError("A array is a read-only object");
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

	public JexlRecord find(String field, Object value) {
		if (field != null && value != null) {
			for (Object o : this) {
				if (o instanceof JexlRecord r) {
					Object v = r.get(field);
					if (value.equals(v)) {
						return r;
					}
				}
			}
		}
		return null;
	}

	@Override
	public Map<String, Object> toMap() {
		if (map == null) {
			map = new HashMap<>();
			for (int i = 0; i < size(); i++) {
				Object value = super.get(i);
				if (value instanceof AvroContainer c) {
					map.put(String.valueOf(i), c.toMap());
				} else {
					map.put(String.valueOf(i), value);
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
	public boolean has(String name) {
		if ("parent".equals(name)) {
			return true;
		} else {
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
	}

}
package io.rtdi.bigdata.rulesservice.rules;

import org.apache.avro.Schema;

public abstract class RuleWithName extends Rule {
	private String rulename;

	public RuleWithName() {
		super();
	}

	public RuleWithName(String fieldname, String rulename, Schema schema) {
		super(fieldname, schema != null ? schema.getFullName() : null);
		this.rulename = rulename;
	}

	public String getRulename() {
		return rulename;
	}

	public void setRulename(String rulename) {
		this.rulename = rulename;
	}

	@Override
	protected String getRulePath() {
		if (rulepath == null) {
			return rulename;
		} else {
			return rulepath;
		}
	}

	@Override
	public void setParentPath(String parentpath) {
		if (parentpath != null) {
			rulepath = parentpath + "/" + getRulename();
		} else {
			rulepath = getRulename();
		}
	}

}

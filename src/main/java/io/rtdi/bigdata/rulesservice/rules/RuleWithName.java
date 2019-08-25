package io.rtdi.bigdata.rulesservice.rules;

public abstract class RuleWithName<R extends Rule<?>> extends Rule<R> {
	private String rulename;

	public RuleWithName() {
		super();
	}

	public RuleWithName(String fieldname, String rulename) {
		super(fieldname);
		this.rulename = rulename;
	}

	public String getRulename() {
		return rulename;
	}

	public void setRulename(String rulename) {
		this.rulename = rulename;
	}

}

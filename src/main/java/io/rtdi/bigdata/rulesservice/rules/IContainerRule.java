package io.rtdi.bigdata.rulesservice.rules;

import java.util.List;

/**
 * A rule for a record, array or union, any schema that can have complex children
 */
public interface IContainerRule {

	List<Rule> getRules();
	void addRule(Rule r);
	void update(IContainerRule empty);

}

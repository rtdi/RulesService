package io.rtdi.bigdata.rulesservice;

import io.rtdi.bigdata.connector.connectorframework.JerseyApplication;

public class JerseyApplicationRulesService extends JerseyApplication {

	public JerseyApplicationRulesService() {
		super();
	}

	@Override
	protected String[] getPackages() {
		return new String[] {"io.rtdi.bigdata.rulesservice.rest"};
	}

}

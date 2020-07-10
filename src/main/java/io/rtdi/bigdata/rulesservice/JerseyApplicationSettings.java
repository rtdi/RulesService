package io.rtdi.bigdata.rulesservice;

import io.rtdi.bigdata.connector.connectorframework.JerseyApplication;

public class JerseyApplicationSettings extends JerseyApplication {

	public JerseyApplicationSettings() {
		super();
	}

	@Override
	protected String[] getPackages() {
		return new String[] {"io.rtdi.bigdata.rulesservice.rest"};
	}

}

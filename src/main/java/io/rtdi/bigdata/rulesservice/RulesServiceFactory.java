package io.rtdi.bigdata.rulesservice;

import java.io.IOException;

import io.rtdi.bigdata.connector.connectorframework.BrowsingService;
import io.rtdi.bigdata.connector.connectorframework.ConnectorFactory;
import io.rtdi.bigdata.connector.connectorframework.IConnectorFactoryService;
import io.rtdi.bigdata.connector.connectorframework.Service;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectionController;
import io.rtdi.bigdata.connector.connectorframework.controller.ServiceController;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ConnectionProperties;
import io.rtdi.bigdata.connector.properties.ServiceProperties;

public class RulesServiceFactory extends ConnectorFactory<ConnectionProperties> implements IConnectorFactoryService {

	public RulesServiceFactory() {
		super("RulesService");
	}

	@Override
	public ConnectionProperties createConnectionProperties(String name) throws PropertiesException {
		return null;
	}

	@Override
	public BrowsingService<ConnectionProperties> createBrowsingService(ConnectionController controller) throws IOException {
		return null;
	}

	@Override
	public Service createService(ServiceController instance) throws PropertiesException {
		return new RulesService(instance);
	}

	@Override
	public ServiceProperties createServiceProperties(String servicename) throws PropertiesException {
		return new RulesServiceProperties(servicename);
	}

	@Override
	public boolean supportsBrowsing() {
		return false;
	}

}

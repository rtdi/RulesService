package io.rtdi.bigdata.rulesservice;

import java.io.IOException;

import io.rtdi.bigdata.connector.connectorframework.BrowsingService;
import io.rtdi.bigdata.connector.connectorframework.ConnectorFactory;
import io.rtdi.bigdata.connector.connectorframework.Consumer;
import io.rtdi.bigdata.connector.connectorframework.Producer;
import io.rtdi.bigdata.connector.connectorframework.Service;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectionController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConsumerInstanceController;
import io.rtdi.bigdata.connector.connectorframework.controller.ProducerInstanceController;
import io.rtdi.bigdata.connector.connectorframework.controller.ServiceController;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ConnectionProperties;
import io.rtdi.bigdata.connector.properties.ConsumerProperties;
import io.rtdi.bigdata.connector.properties.ProducerProperties;
import io.rtdi.bigdata.connector.properties.ServiceProperties;

public class RulesServiceFactory extends ConnectorFactory<ConnectionProperties, ProducerProperties, ConsumerProperties> {

	public RulesServiceFactory() {
		super("RulesService");
	}

	@Override
	public Consumer<ConnectionProperties, ConsumerProperties> createConsumer(ConsumerInstanceController instance) throws IOException {
		return null;
	}

	@Override
	public Producer<ConnectionProperties, ProducerProperties> createProducer(ProducerInstanceController instance) throws IOException {
		return null;
	}

	@Override
	public ConnectionProperties createConnectionProperties(String name) throws PropertiesException {
		return null;
	}

	@Override
	public ConsumerProperties createConsumerProperties(String name) throws PropertiesException {
		return null;
	}

	@Override
	public ProducerProperties createProducerProperties(String name) throws PropertiesException {
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
	public boolean supportsConnections() {
		return false;
	}

	@Override
	public boolean supportsServices() {
		return true;
	}

	@Override
	public boolean supportsProducers() {
		return false;
	}

	@Override
	public boolean supportsConsumers() {
		return false;
	}

	@Override
	public boolean supportsBrowsing() {
		return false;
	}

}

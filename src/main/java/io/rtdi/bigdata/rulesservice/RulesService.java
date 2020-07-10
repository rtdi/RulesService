package io.rtdi.bigdata.rulesservice;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.rtdi.bigdata.connector.connectorframework.Service;
import io.rtdi.bigdata.connector.connectorframework.controller.ServiceController;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ServiceEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.pipeline.foundation.utils.IOUtils;

public class RulesService extends Service {

	public RulesService(ServiceController controller) throws PropertiesException {
		super(controller);
	}

	@Override
	public void updateLandscape() {
		RulesServiceProperties props = (RulesServiceProperties) getController().getServiceProperties();
		List<String> consumertopics = Collections.singletonList(props.getSourceTopic());
		
		Map<String, Set<String>> producertopics = new HashMap<>();
		Set<String> usedschemas = new HashSet<>();
		
		if (props.getMicroServices() != null) {
			for (RuleStep m : props.getMicroServices()) {
				for ( String schema : m.getSchemaRules().keySet()) {
					usedschemas.add(schema);
				}
			}
			
			producertopics.put(props.getTargetTopic(), usedschemas);
			
			try {
				getController().getPipelineAPI().addServiceMetadata(
						new ServiceEntity(
								getController().getName(),
								IOUtils.getHostname(),
								getController().getPipelineAPI().getConnectionLabel(),
								consumertopics,
								producertopics));
			} catch (IOException e) {
				logger.error("Failed to update the Landscape metadata in the server", e);
			}
		}
	}

	@Override
	public void updateSchemaCache() {
	}

}

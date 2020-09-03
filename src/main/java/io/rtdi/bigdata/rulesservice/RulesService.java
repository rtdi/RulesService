package io.rtdi.bigdata.rulesservice;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

import io.rtdi.bigdata.connector.connectorframework.Service;
import io.rtdi.bigdata.connector.connectorframework.controller.ServiceController;
import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineAPI;
import io.rtdi.bigdata.connector.pipeline.foundation.MicroServiceTransformation;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaConstants;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ServiceEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.SchemaException;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.ValueSchema;
import io.rtdi.bigdata.connector.pipeline.foundation.utils.IOUtils;

public class RulesService extends Service {

	public RulesService(ServiceController controller) throws PropertiesException {
		super(controller);
		/*
		 * Validate that the last schema version has the AUDIT structure where rules write their results into.
		 * If data was produced via this framework it will, but data produced via other means might not. 
		 */
		RulesServiceProperties props = (RulesServiceProperties) controller.getServiceProperties();
		IPipelineAPI<?, ?, ?, ?> api = controller.getPipelineAPI();
		if (props.getMicroserviceTransformations() != null) {
			for (MicroServiceTransformation microservice : props.getMicroserviceTransformations()) {
				RuleStep rulestep = (RuleStep) microservice;
				for (SchemaRuleSet rs : rulestep.getSchemaRules().values()) {
					SchemaHandler handler = api.getSchema(rs.getSchemaName());
					Schema valueschema = handler.getValueSchema();
					if (valueschema.getField(ValueSchema.AUDIT) == null) {
						api.registerSchema(handler.getSchemaName(), null, handler.getKeySchema(), createNewSchemaVersion(valueschema));
					}
				}
			}
		}
	}

	private Schema createNewSchemaVersion(Schema schema) throws PropertiesException {
		try {
			ValueSchema n = new ValueSchema(schema.getName(), schema.getNamespace(), schema.getDoc());
			for (Field f : schema.getFields()) {
				switch (f.name()) {
				case SchemaConstants.SCHEMA_COLUMN_CHANGE_TYPE:
				case SchemaConstants.SCHEMA_COLUMN_CHANGE_TIME:
				case SchemaConstants.SCHEMA_COLUMN_SOURCE_ROWID:
				case SchemaConstants.SCHEMA_COLUMN_SOURCE_TRANSACTION:
				case SchemaConstants.SCHEMA_COLUMN_SOURCE_SYSTEM:
				case SchemaConstants.SCHEMA_COLUMN_EXTENSION:
				case ValueSchema.AUDIT:
					// do nothing, use the ValueSchema schema definition for the fixed columns
				default:
					n.add(f);
				}
			}
			n.build();
			return n.getSchema();
		} catch (SchemaException e) {
			throw new PropertiesException("The most recent schema version does not have a " + ValueSchema.AUDIT + " field - tried to derive a new schema and failed", e);
		}
	}

	@Override
	public void updateLandscape() {
		RulesServiceProperties props = (RulesServiceProperties) getController().getServiceProperties();
		List<String> consumertopics = Collections.singletonList(props.getSourceTopic());
		
		Map<String, Set<String>> producertopics = new HashMap<>();
		Set<String> usedschemas = new HashSet<>();
		
		if (props.getMicroserviceTransformations() != null) {
			for ( MicroServiceTransformation microservice : props.getMicroserviceTransformations()) {
				RuleStep rulestep = (RuleStep) microservice;
				for (SchemaRuleSet rs : rulestep.getSchemaRules().values()) {
					usedschemas.add(rs.getSchemaName().getName());
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

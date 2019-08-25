package io.rtdi.bigdata.rulesservice.rest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.annotation.security.RolesAllowed;
import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import io.rtdi.bigdata.connector.connectorframework.WebAppController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;
import io.rtdi.bigdata.connector.connectorframework.controller.ServiceController;
import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.connector.connectorframework.rest.JAXBErrorResponseBuilder;
import io.rtdi.bigdata.connector.connectorframework.rest.JAXBSuccessResponseBuilder;
import io.rtdi.bigdata.connector.connectorframework.servlet.ServletSecurityConstants;
import io.rtdi.bigdata.connector.pipeline.foundation.MicroServiceTransformation;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaHandler;
import io.rtdi.bigdata.rulesservice.RuleStep;
import io.rtdi.bigdata.rulesservice.SchemaRuleSet;


@Path("/")
public class RulesEndpoint {
	
	@Context
    private Configuration configuration;

	@Context 
	private ServletContext servletContext;
		
	@GET
	@Path("/rules/{servicename}/{microservicename}")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
    public Response getSchemas(@PathParam("servicename") String servicename, @PathParam("microservicename") String microservicename) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ServiceController service = connector.getServiceOrFail(servicename);
			MicroServiceTransformation m = service.getMicroserviceOrFail(microservicename);
			RuleStep step = (RuleStep) m;
			return Response.ok(new SchemaList(step)).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@GET
	@Path("/rules/{servicename}/{microservicename}/{schemaname}")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
    public Response getRules(@PathParam("servicename") String servicename, 
    		@PathParam("microservicename") String microservicename, 
    		@PathParam("schemaname") String schemaname) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ServiceController service = connector.getServiceOrFail(servicename);
			MicroServiceTransformation m = service.getMicroserviceOrFail(microservicename);
			RuleStep step = (RuleStep) m;
			SchemaRuleSet data = step.getSchemaRuleOrFail(schemaname);
			SchemaHandler handler = connector.getPipelineAPI().getSchema(schemaname);
			if (handler != null) {
				return Response.ok(data.createUIRuleTree(handler.getValueSchema())).build();
			} else {
				throw new ConnectorCallerException("No schema with that name exists", null, schemaname);
			}
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}


	@POST
	@Path("/rules/{servicename}/{microservicename}/{schemaname}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_CONFIG)
    public Response setRules(@PathParam("servicename") String servicename, 
    		@PathParam("microservicename") String microservicename, 
    		@PathParam("schemaname") String schemaname, 
    		SchemaRuleSet data) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ServiceController service = connector.getService(servicename);
			return JAXBSuccessResponseBuilder.getJAXBResponse("created");
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@DELETE
	@Path("/rules/{servicename}/{microservicename}/{schemaname}")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_CONFIG)
    public Response deleteRules(@PathParam("servicename") String servicename, 
    		@PathParam("microservicename") String microservicename, 
    		@PathParam("schemaname") String schemaname) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ServiceController service = connector.getServiceOrFail(servicename);
			connector.removeService(service);
			return JAXBSuccessResponseBuilder.getJAXBResponse("deleted");
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	public static class SchemaList {

		private List<SchemaNameEntity> schemas;
		
		public SchemaList(RuleStep step) {
			step.getSchemaRules().keySet();

			if (step.getSchemaRules() != null) {
				Collection<String> serviceset = step.getSchemaRules().keySet();
				this.schemas = new ArrayList<>();
				for (String service : serviceset) {
					this.schemas.add(new SchemaNameEntity(service));
				}
			}
		}
		
		public List<SchemaNameEntity> getSchemas() {
			return schemas;
		}
		
	}
	
	public static class SchemaNameEntity {

		private String name;

		public SchemaNameEntity(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}
	}
	
}

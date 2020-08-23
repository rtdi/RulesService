package io.rtdi.bigdata.rulesservice.rest;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.annotation.security.RolesAllowed;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import io.rtdi.bigdata.connector.connectorframework.WebAppController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;
import io.rtdi.bigdata.connector.connectorframework.controller.ServiceController;
import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.connector.connectorframework.rest.JAXBErrorResponseBuilder;
import io.rtdi.bigdata.connector.connectorframework.rest.JAXBSuccessResponseBuilder;
import io.rtdi.bigdata.connector.connectorframework.servlet.ServletSecurityConstants;
import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineAPI;
import io.rtdi.bigdata.connector.pipeline.foundation.MicroServiceTransformation;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaRegistryName;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicName;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicPayload;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.rulesservice.RuleStep;
import io.rtdi.bigdata.rulesservice.SchemaRuleSet;


@Path("/")
public class RulesEndpoint {
	
	private static final String SAMPLEDATACACHE = "sample.data.cache";

	@Context
    private Configuration configuration;

	@Context 
	private ServletContext servletContext;
	
	@Context 
	HttpServletRequest request;
		
	@GET
	@Path("/rules/{servicename}")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
    public Response getSchemas(
    		@PathParam("servicename") String servicename) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ServiceController service = connector.getServiceOrFail(servicename);
			return Response.ok(new SchemaList(service.getSchemas())).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@GET
	@Path("/rules/{servicename}/{schemaname}/{microservicename}")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
    public Response getRules(
    		@PathParam("servicename") String servicename, 
    		@PathParam("schemaname") String schemaname,
    		@PathParam("microservicename") String microservicename) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ServiceController service = connector.getServiceOrFail(servicename);
			MicroServiceTransformation m = service.getMicroserviceOrFail(schemaname, microservicename);
			RuleStep step = (RuleStep) m;
			SchemaRuleSet data = step.getSchemaRule();
			SchemaHandler handler = connector.getPipelineAPI().getSchema(SchemaRegistryName.create(schemaname));
			if (handler == null) {
				throw new ConnectorCallerException("No schema with that name exists", null, null, schemaname);
			} else {
				if (data == null) {
					data = SchemaRuleSet.createUIRuleTree(schemaname, handler.getValueSchema(), null);
				} else {
					data.updateSchema(handler.getValueSchema());
				}
				JexlRecord sampledata = getSampleData(
						service.getServiceProperties().getSourceTopic(),
						schemaname,
						connector.getPipelineAPI(),
						request);
				data.assignSamplevalue(sampledata);
				data.validateRule(sampledata);
				return Response.ok(data).build();
			}
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@POST
	@Path("/rules/{servicename}/{schemaname}/{microservicename}/validate")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
    public Response getValidation(@PathParam("servicename") String servicename, 
    		@PathParam("schemaname") String schemaname,
    		@PathParam("microservicename") String microservicename, 
    		SchemaRuleSet data) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ServiceController service = connector.getServiceOrFail(servicename);
			JexlRecord sampledata = getSampleData(
					service.getServiceProperties().getSourceTopic(),
					schemaname,
					connector.getPipelineAPI(),
					request);
			data.assignSamplevalue(sampledata);
			data.validateRule(sampledata);
			return Response.ok(data).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	private static JexlRecord getSampleData(String topicname, String schemaname, IPipelineAPI<?,?,?,?> api, HttpServletRequest req) throws IOException {
		HttpSession session = req.getSession(false);
		if (session != null) {
			@SuppressWarnings("unchecked")
			Cache<String, JexlRecord> cache = (Cache<String, JexlRecord>) session.getAttribute(SAMPLEDATACACHE);
			if (cache == null) {
				cache = Caffeine.newBuilder().expireAfterAccess(Duration.ofMinutes(30)).maximumSize(1000).build();
				session.setAttribute(SAMPLEDATACACHE, cache);
			}
			String cachekey = topicname + "_" + schemaname;
			JexlRecord sampledata = cache.getIfPresent(cachekey);
			if (sampledata == null) {
				List<TopicPayload> l = api.getAllRecordsSince(TopicName.create(topicname), System.currentTimeMillis()-3600000L, 1, SchemaRegistryName.create(schemaname));
				if (l != null && l.size() != 0) {
					sampledata = l.get(0).getValueRecord();
				}
				cache.put(cachekey, sampledata);
			}
			
			return sampledata;
		} else {
			return null;
		}
	}

	@POST
	@Path("/rules/{servicename}/{schemaname}/{microservicename}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_CONFIG)
    public Response setRules(@PathParam("servicename") String servicename, 
    		@PathParam("schemaname") String schemaname, 
    		@PathParam("microservicename") String microservicename, 
    		SchemaRuleSet data) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ServiceController service = connector.getService(servicename);
			MicroServiceTransformation m = service.getMicroserviceOrFail(schemaname, microservicename);
			RuleStep step = (RuleStep) m;
			java.nio.file.Path p = service.getDirectory().toPath();
			File directory = p.resolve(schemaname).resolve(m.getName()).toFile();
			if (!directory.exists()) {
				if (!directory.mkdirs()) {
					throw new ConnectorCallerException("Cannot create directory for the microservice schema", null, null, directory.getAbsolutePath());
				}
			} else if (!directory.isDirectory()) {
				throw new ConnectorCallerException("There is a file of that name already", null, null, directory.getAbsolutePath());
			}
			data.write(directory);
			step.setSchemaRuleSet(data);
			return JAXBSuccessResponseBuilder.getJAXBResponse("created");
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	/* @DELETE
	@Path("/rules/{servicename}/{schemaname}/{microservicename}")
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
	} */

	public static class SchemaList {

		private List<SchemaNameEntity> schemas;

		public SchemaList() {
		}
		
		public SchemaList(Set<String> schemalist) {
			if (schemalist != null) {
				this.schemas = new ArrayList<>();
				for (String s : schemalist) {
					this.schemas.add(new SchemaNameEntity(s));
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

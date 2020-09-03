package io.rtdi.bigdata.rulesservice.rest;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.security.RolesAllowed;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
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
import io.rtdi.bigdata.connector.pipeline.foundation.utils.FileNameEncoder;
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
			return Response.ok(new MicroserviceList(service.getMicroservices())).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@GET
	@Path("/rules/{servicename}/{microservicename}")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
    public Response getRuleSchemas(
    		@PathParam("servicename") String servicename,
    		@PathParam("microservicename") String microservicename) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ServiceController service = connector.getServiceOrFail(servicename);
			/*
			 * In case this is a brand new microservice, it is not attached to the step yet.
			 */
			MicroServiceTransformation m = service.getMicroservice(microservicename);
			Collection<SchemaRuleSet> existingschemas = null;
			if (m != null) {
				RuleStep step = (RuleStep) m;
				Map<String, SchemaRuleSet> data = step.getSchemaRules();
				existingschemas = data.values();
			}
			SchemaListNameEntity ret = new SchemaListNameEntity(connector.getPipelineAPI(), existingschemas);
			return Response.ok(ret).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@GET
	@Path("/rules/{servicename}/{microservicename}/{schemaname}")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
    public Response getRules(
    		@PathParam("servicename") String servicename,
    		@PathParam("microservicename") String microservicename, 
    		@PathParam("schemaname") String schemaname) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ServiceController service = connector.getServiceOrFail(servicename);
			MicroServiceTransformation m = service.getMicroservice(microservicename);
			SchemaRuleSet data = null;
			if (m != null) {
				RuleStep step = (RuleStep) m;
				data = step.getSchemaRule(SchemaRegistryName.create(schemaname));
			}
			SchemaHandler handler = connector.getPipelineAPI().getSchema(SchemaRegistryName.create(schemaname));
			if (handler == null) {
				throw new ConnectorCallerException("No schema with that name exists", null, null, schemaname);
			} else {
				if (data == null) {
					data = SchemaRuleSet.createUIRuleTree(SchemaRegistryName.create(schemaname), schemaname, handler.getValueSchema(), null);
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
	@Path("/rules/{servicename}/{microservicename}/{schemaname}/validate")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
    public Response getValidation(@PathParam("servicename") String servicename, 
    		@PathParam("microservicename") String microservicename, 
    		@PathParam("schemaname") String schemaname,
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
				if (l != null && l.size() != 0) { // might be that the schema has no data in this topic
					sampledata = l.get(0).getValueRecord();
					cache.put(cachekey, sampledata);
				}
			}
			
			return sampledata;
		} else {
			return null;
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
			MicroServiceTransformation m = service.getMicroservice(microservicename);
			RuleStep step;
			if (m == null) {
				m = new RuleStep(microservicename);
				service.getServiceProperties().addMicroService(m);
			}
			step = (RuleStep) m;
			java.nio.file.Path p = service.getDirectory().toPath();
			File directory = p.resolve(FileNameEncoder.encodeName(m.getName())).toFile();
			if (!directory.exists()) {
				if (!directory.mkdirs()) {
					throw new ConnectorCallerException("Cannot create directory for the microservice schema", null, null, directory.getAbsolutePath());
				}
			} else if (!directory.isDirectory()) {
				throw new ConnectorCallerException("There is a file of that name already", null, null, directory.getAbsolutePath());
			}
			if (data != null) {
				SchemaRegistryName name = SchemaRegistryName.create(schemaname);
				data.setSchemaName(name);
				data.write(directory);
				step.addSchemaRuleSet(data);
			}
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
			RuleStep microservice = (RuleStep) service.getMicroserviceOrFail(microservicename);
			java.nio.file.Path p = service.getDirectory().toPath();
			File directory = p.resolve(schemaname).resolve(microservice.getName()).toFile();
			microservice.deleteSchemaRuleSetDefinition(SchemaRegistryName.create(schemaname), directory);
			return JAXBSuccessResponseBuilder.getJAXBResponse("deleted");
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	public static class MicroserviceList {

		private List<MicroserviceNameEntity> schemas;

		public MicroserviceList() {
		}
		
		public MicroserviceList(Set<MicroServiceTransformation> set) {
			if (set != null) {
				this.schemas = new ArrayList<>();
				for (MicroServiceTransformation s : set) {
					this.schemas.add(new MicroserviceNameEntity(s.getName()));
				}
			}
		}
		
		public List<MicroserviceNameEntity> getSchemas() {
			return schemas;
		}
		
	}
	
	public static class MicroserviceNameEntity {

		private String name;

		public MicroserviceNameEntity(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}
	}
	
}

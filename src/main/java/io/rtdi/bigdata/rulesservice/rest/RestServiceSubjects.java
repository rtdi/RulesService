package io.rtdi.bigdata.rulesservice.rest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.rtdi.bigdata.rulesservice.LoggingUtil;
import io.rtdi.bigdata.rulesservice.RulesService;
import io.rtdi.bigdata.rulesservice.config.RuleFileDefinition;
import io.rtdi.bigdata.rulesservice.config.SubjectName;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import jakarta.annotation.security.RolesAllowed;
import jakarta.servlet.ServletContext;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Configuration;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;

@Path("/")
public class RestServiceSubjects {
	protected static final int SAMPLE_MAX_ROWS = 100;

	@Context
	private Configuration configuration;

	@Context
	private ServletContext servletContext;

	@Context
	HttpServletRequest request;

	@GET
	@Path("/subjects")
	@Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
	@Operation(
			summary = "All subjects",
			description = "Returns a list of all subjects from the schema registry",
			responses = {
					@ApiResponse(
							responseCode = "200",
							description = "List of subjects",
							content = {
									@Content(
											array = @ArraySchema(schema = @Schema(implementation = SubjectName.class))
											)
							}
							),
					@ApiResponse(
							responseCode = "500",
							description = "Any exception thrown",
							content = {
									@Content(
											schema = @Schema(implementation = ErrorResponse.class)
											)
							}
							)
			})
	public Response getSubjects() {
		try {
			LoggingUtil.logRequestBegin(request);
			RulesService service = RulesService.getRulesService(servletContext);
			Collection<String> subjects = service.getSubjects();
			List<SubjectName> ret = new ArrayList<>();
			for (String s : subjects) {
				ret.add(new SubjectName(s));
			}
			return LoggingUtil.requestEnd(request, ret);
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(request, e);
		}
	}

	@POST
	@Path("/subjects/{subject}/update")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_EDIT)
	@Operation(
			summary = "Update a rule file to the latest schema",
			description = "Takes the current rule file and updates it to the latest schema version",
			responses = {
					@ApiResponse(
							responseCode = "200",
							description = "New rule file",
							content = {
									@Content(
											schema = @Schema(implementation = RuleFileDefinition.class)
											)
							}
							),
					@ApiResponse(
							responseCode = "500",
							description = "Any exception thrown",
							content = {
									@Content(
											schema = @Schema(implementation = ErrorResponse.class)
											)
							}
							)
			})
	public Response updateRuleFile(
			@PathParam("subject")
			@Parameter(
					description = "name of subject",
					example = "order_data-value"
					)
			String subjectname,
			@RequestBody RuleFileDefinition input) {
		try {
			LoggingUtil.logRequestBegin(request);
			RulesService service = RulesService.getRulesService(servletContext);
			RuleFileDefinition empty = RuleFileDefinition.createEmptyRuleFileDefinition(subjectname, service);
			input.update(empty);
			return LoggingUtil.requestEnd(request, input);
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(request, e);
		}
	}


	@GET
	@Path("/subjects/{subject}")
	@Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
	@Operation(
			summary = "Get the schema definition",
			description = "Returns the schema definition from the schema registry",
			responses = {
					@ApiResponse(
							responseCode = "200",
							description = "schema metadata",
							content = {
									@Content(
											schema = @Schema(implementation = Schema.class)
											)
							}
							),
					@ApiResponse(
							responseCode = "500",
							description = "Any exception thrown",
							content = {
									@Content(
											schema = @Schema(implementation = ErrorResponse.class)
											)
							}
							)
			})
	public Response getSchemaDefiniton(
			@PathParam("subject")
			@Parameter(
					description = "name of value subject",
					example = "order_data"
					)
			String subjectname) {
		try {
			LoggingUtil.logRequestBegin(request);
			RulesService service = RulesService.getRulesService(servletContext);
			org.apache.avro.Schema schema = service.getLatestSchema(subjectname);
			return LoggingUtil.requestEnd(request, schema);
		} catch (RestClientException e) {
			if (e.getStatus() == 404) {
				// invalid schema
				return LoggingUtil.requestEndInputError(request, e, Status.NOT_FOUND);
			} else {
				return LoggingUtil.requestEndTechnicalError(request, e);
			}
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(request, e);
		}
	}

}

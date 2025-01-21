package io.rtdi.bigdata.rulesservice.rest;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.rtdi.bigdata.rulesservice.LoggingUtil;
import io.rtdi.bigdata.rulesservice.RulesService;
import io.rtdi.bigdata.rulesservice.config.RuleFileDefinition;
import io.rtdi.bigdata.rulesservice.config.RuleFileName;
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
import jakarta.ws.rs.PATCH;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Configuration;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

@Path("/")
public class RestServiceRules {
	protected static final int SAMPLE_MAX_ROWS = 100;

	protected final Logger log = LogManager.getLogger(this.getClass().getName());

	@Context
	private Configuration configuration;

	@Context
	private ServletContext servletContext;

	@Context
	HttpServletRequest request;


	@GET
	@Path("/subjects/{subject}/rules")
	@Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
	@Operation(
			summary = "All rules for a subject",
			description = "Returns a list of all rule groups using that subject is using as input from the file system",
			responses = {
					@ApiResponse(
							responseCode = "200",
							description = "List of all rule group names",
							content = {
									@Content(
											array = @ArraySchema(schema = @Schema(implementation = RuleFileName.class))
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
	public Response getRuleFiles(
			@PathParam("subject")
			@Parameter(
					description = "name of subject",
					example = "order_data-value"
					)
			String subjectname) {
		try {
			LoggingUtil.logRequestBegin(log, request);
			RulesService service = RulesService.getRulesService(servletContext);
			List<RuleFileName> rulegroups = service.getRuleFiles(subjectname);
			return LoggingUtil.requestEnd(log, request, rulegroups);
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(log, request, e);
		}
	}

	@GET
	@Path("/rules")
	@Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
	@Operation(
			summary = "All rules",
			description = "Returns a list of all active rule groups from the file system",
			responses = {
					@ApiResponse(
							responseCode = "200",
							description = "List of all rule group names",
							content = {
									@Content(
											array = @ArraySchema(schema = @Schema(implementation = RuleFileName.class))
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
	public Response getAllRuleFiles() {
		try {
			LoggingUtil.logRequestBegin(log, request);
			RulesService service = RulesService.getRulesService(servletContext);
			List<RuleFileName> rulegroups = service.getAllRuleFiles();
			return LoggingUtil.requestEnd(log, request, rulegroups);
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(log, request, e);
		}
	}

	@GET
	@Path("/subjects/{subject}/rules/{path:.*}")
	@Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
	@Operation(
			summary = "Get a rule group definition",
			description = "Returns the rule group definition from the file system",
			responses = {
					@ApiResponse(
							responseCode = "200",
							description = "List of all rule group names",
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
	public Response getRuleFile(
			@PathParam("path")
			@Parameter(
					description = "name of the rule group file",
					example = "ruleset1/address_rules.json"
					)
			String path,
			@PathParam("subject")
			@Parameter(
					description = "name of subject",
					example = "order_data-value"
					)
			String subjectname) {
		try {
			LoggingUtil.logRequestBegin(log, request);
			RulesService service = RulesService.getRulesService(servletContext);
			java.nio.file.Path name = java.nio.file.Path.of(path);
			RuleFileDefinition rg = RuleFileDefinition.load(service.getRuleFileRootDir(), subjectname, name, false);
			return LoggingUtil.requestEnd(log, request, rg);
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(log, request, e);
		}
	}

	@POST
	@Path("/subjects/{subject}/rules/{path:.*}")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_EDIT)
	@Operation(
			summary = "Save a rule file definition",
			description = "Saves the posted rule file, a pure file system operation",
			responses = {
					@ApiResponse(
							responseCode = "200",
							description = "Success message",
							content = {
									@Content(
											schema = @Schema(implementation = SuccessResponse.class)
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
	public Response postRuleFile(
			@PathParam("path")
			@Parameter(
					description = "name of the rule group file to save; note that this can be different from the name inside the payload",
					example = "ruleset1/address_rules.json"
					)
			String path,
			@PathParam("subject")
			@Parameter(
					description = "name of value subject",
					example = "order_data"
					)
			String subjectname,
			@RequestBody RuleFileDefinition input) {
		try {
			LoggingUtil.logRequestBegin(log, request);
			RulesService service = RulesService.getRulesService(servletContext);
			java.nio.file.Path name = java.nio.file.Path.of(path); // The name of the file to be saved, which is the old name
			input.setActive(false);
			input.setInputsubjectname(subjectname);
			input.save(service.getRuleFileRootDir(), name);
			return LoggingUtil.requestEnd(log, request, SuccessResponse.SUCCESS);
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(log, request, e);
		}
	}

	@PATCH
	@Path("/subjects/{subject}/rules/{path:.*}")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_EDIT)
	@Operation(
			summary = "Active a rule file",
			description = "Returns a list of all rule groups using that subject is using as input, a pure file system operation",
			responses = {
					@ApiResponse(
							responseCode = "200",
							description = "List of all rule group names",
							content = {
									@Content(
											schema = @Schema(implementation = SuccessResponse.class)
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
	public Response activateRuleFile(
			@PathParam("path")
			@Parameter(
					description = "name of the rule group file to save; note that this can be different from the name inside the payload",
					example = "ruleset1/address_rules.json"
					)
			String path,
			@PathParam("subject")
			@Parameter(
					description = "name of value subject",
					example = "order_data"
					)
			String subjectname) {
		try {
			LoggingUtil.logRequestBegin(log, request);
			RulesService service = RulesService.getRulesService(servletContext);
			RuleFileDefinition.copyToActivate(service.getRuleFileRootDir(), subjectname, path);
			return LoggingUtil.requestEnd(log, request, SuccessResponse.SUCCESS);
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(log, request, e);
		}
	}

	@GET
	@Path("/subjects/{subject}/defaultrule")
	@Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
	@Operation(
			summary = "Create an empty rule group definition",
			description = "Create a new rule group for that subject",
			responses = {
					@ApiResponse(
							responseCode = "200",
							description = "List of all rule group names",
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
	public Response createEmptyRuleFile(
			@PathParam("subject")
			@Parameter(
					description = "name of subject",
					example = "order_data-value"
					)
			String subjectname) {
		try {
			LoggingUtil.logRequestBegin(log, request);
			RulesService service = RulesService.getRulesService(servletContext);
			RuleFileDefinition rg = RuleFileDefinition.createEmptyRuleFileDefinition(subjectname, service);
			return LoggingUtil.requestEnd(log, request, rg);
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(log, request, e);
		}
	}

}

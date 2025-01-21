package io.rtdi.bigdata.rulesservice.rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.rtdi.bigdata.rulesservice.LoggingUtil;
import io.rtdi.bigdata.rulesservice.RulesService;
import io.rtdi.bigdata.rulesservice.config.RuleFileDefinition;
import io.rtdi.bigdata.rulesservice.config.RuleStep;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import jakarta.annotation.security.RolesAllowed;
import jakarta.servlet.ServletContext;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Configuration;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

@Path("/")
public class RestServiceCalculate {
	protected static final int SAMPLE_MAX_ROWS = 100;

	protected final Logger log = LogManager.getLogger(this.getClass().getName());

	@Context
	private Configuration configuration;

	@Context
	private ServletContext servletContext;

	@Context
	HttpServletRequest request;


	@POST
	@Path("/calculate/{stepindex}")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_EDIT)
	@Operation(
			summary = "apply the provided sample data for this step",
			description = "post the entire rule file but process just a single step using its sample value",
			responses = {
					@ApiResponse(
							responseCode = "200",
							description = "Success message",
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
	public Response processStep(
			@PathParam("stepindex")
			@Parameter(
					description = "index of the step to process",
					example = "0"
					)
			Integer stepindex,
			@RequestBody RuleFileDefinition input) {
		try {
			LoggingUtil.logRequestBegin(log, request);
			RulesService service = RulesService.getRulesService(servletContext);
			RuleStep ret = service.applySampleData(input, stepindex);
			return LoggingUtil.requestEnd(log, request, ret);
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(log, request, e);
		}
	}

	@POST
	@Path("/calculate")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_EDIT)
	@Operation(
			summary = "Apply the selected sample file to the rule file",
			description = "Calculate all rules using the specified rule file",
			responses = {
					@ApiResponse(
							responseCode = "200",
							description = "The rule file with the new sample values",
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
	public Response processAll(
			@RequestBody RuleFileDefinition input) {
		try {
			LoggingUtil.logRequestBegin(log, request);
			RulesService service = RulesService.getRulesService(servletContext);
			RuleFileDefinition ret = service.applySampleFile(input);
			return LoggingUtil.requestEnd(log, request, ret);
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(log, request, e);
		}
	}

}

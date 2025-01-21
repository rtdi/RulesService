package io.rtdi.bigdata.rulesservice.rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.rtdi.bigdata.rulesservice.LoggingUtil;
import io.rtdi.bigdata.rulesservice.RulesService;
import io.rtdi.bigdata.rulesservice.config.ServiceSettings;
import io.rtdi.bigdata.rulesservice.config.ServiceStatus;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import jakarta.annotation.security.RolesAllowed;
import jakarta.servlet.ServletContext;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Configuration;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

@Path("/")
public class RestServiceConfig {
	protected static final int SAMPLE_MAX_ROWS = 100;

	protected final Logger log = LogManager.getLogger(this.getClass().getName());

	@Context
	private Configuration configuration;

	@Context
	private ServletContext servletContext;

	@Context
	HttpServletRequest request;

	@GET
	@Path("/config")
	@Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
	@Operation(
			summary = "Return the service status",
			description = "Returns the status of the rules service",
			responses = {
					@ApiResponse(
							responseCode = "200",
							description = "Configuration data",
							content = {
									@Content(
											schema = @Schema(implementation = RestServiceConfig.class)
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
	public Response getConfig() {
		try {
			LoggingUtil.logRequestBegin(log, request);
			RulesService service = RulesService.getRulesService(servletContext);
			boolean admin = request.isUserInRole(ServletSecurityConstants.ROLE_ADMIN);
			ServiceSettings ret = service.getConfig(admin);
			return LoggingUtil.requestEnd(log, request, ret);
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(log, request, e);
		}
	}

	@POST
	@Path("/config")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_ADMIN)
	@Operation(
			summary = "Save the config",
			description = "save the config file and re-establish all connections",
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
	public Response postConfig(@RequestBody ServiceSettings input) {
		try {
			LoggingUtil.logRequestBegin(log, request);
			RulesService service = RulesService.getRulesService(servletContext);
			service.saveConfig(input);
			return LoggingUtil.requestEnd(log, request, new SuccessResponse("Saved"));
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(log, request, e);
		}
	}

	@POST
	@Path("/config/service")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_ADMIN)
	@Operation(
			summary = "Start all services",
			description = "Reads the topic rule config directory and starts all services with at least one active rule",
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
	public Response startService() {
		try {
			LoggingUtil.logRequestBegin(log, request);
			RulesService service = RulesService.getRulesService(servletContext);
			service.startService();
			return LoggingUtil.requestEnd(log, request, new SuccessResponse("Started"));
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(log, request, e);
		}
	}

	@DELETE
	@Path("/config/service")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_ADMIN)
	@Operation(
			summary = "Stops all services",
			description = "Goes through the list of running services and stops them",
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
	public Response stopService() {
		try {
			LoggingUtil.logRequestBegin(log, request);
			RulesService service = RulesService.getRulesService(servletContext);
			service.stopService();
			return LoggingUtil.requestEnd(log, request, new SuccessResponse("Started"));
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(log, request, e);
		}
	}

	@GET
	@Path("/config/service")
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(
			summary = "Return the detailed service status",
			description = "Returns the status of the rules services",
			responses = {
					@ApiResponse(
							responseCode = "200",
							description = "Configuration data",
							content = {
									@Content(
											schema = @Schema(implementation = ServiceStatus.class)
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
	public Response getServiceStatus() {
		try {
			LoggingUtil.logRequestBegin(log, request);
			RulesService service = RulesService.getRulesService(servletContext);
			ServiceStatus ret = service.getServiceStatus();
			return LoggingUtil.requestEnd(log, request, ret);
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(log, request, e);
		}
	}

}

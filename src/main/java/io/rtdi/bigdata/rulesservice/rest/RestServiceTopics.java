package io.rtdi.bigdata.rulesservice.rest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import io.rtdi.bigdata.rulesservice.LoggingUtil;
import io.rtdi.bigdata.rulesservice.RulesService;
import io.rtdi.bigdata.rulesservice.config.TopicName;
import io.rtdi.bigdata.rulesservice.config.TopicRule;
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

@Path("/")
public class RestServiceTopics {
	protected static final int SAMPLE_MAX_ROWS = 100;

	@Context
	private Configuration configuration;

	@Context
	private ServletContext servletContext;

	@Context
	HttpServletRequest request;


	@GET
	@Path("/topicrules")
	@Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
	@Operation(
			summary = "List of topics and their rules",
			description = "Returns a list of all topics and the rules assigned, if there are any",
			responses = {
					@ApiResponse(
							responseCode = "200",
							description = "rules",
							content = {
									@Content(
											array = @ArraySchema(schema = @Schema(implementation = TopicRule.class))
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
	public Response getTopicRules() {
		try {
			LoggingUtil.logRequestBegin(request);
			RulesService service = RulesService.getRulesService(servletContext);
			Collection<TopicRule> topicrules = service.getTopicsAndRules();
			return LoggingUtil.requestEnd(request, topicrules);
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(request, e);
		}
	}

	@POST
	@Path("/topicrules")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_EDIT)
	@Operation(
			summary = "Save the modified rules",
			description = "Go through the input object and save all rules with a modified=true flag",
			responses = {
					@ApiResponse(
							responseCode = "200",
							description = "Success message",
							content = {
									@Content(
											array = @ArraySchema(schema = @Schema(implementation = TopicRule.class))
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
	public Response postRules(@RequestBody List<TopicRule> input) {
		try {
			LoggingUtil.logRequestBegin(request);
			RulesService service = RulesService.getRulesService(servletContext);
			List<TopicRule> ret = service.saveTopicRules(input);
			return LoggingUtil.requestEnd(request, ret);
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(request, e);
		}
	}

	@GET
	@Path("/topics")
	@Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
	@Operation(
			summary = "List of topics",
			description = "Returns a list of all topics",
			responses = {
					@ApiResponse(
							responseCode = "200",
							description = "rules",
							content = {
									@Content(
											array = @ArraySchema(schema = @Schema(implementation = TopicName.class))
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
	public Response getTopics() {
		try {
			LoggingUtil.logRequestBegin(request);
			RulesService service = RulesService.getRulesService(servletContext);
			Collection<String> topics = service.getTopics();
			List<TopicName> topicnames = new ArrayList<>(topics.size());
			for (String t : topics) {
				topicnames.add(new TopicName(t));
			}
			return LoggingUtil.requestEnd(request, topicnames);
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(request, e);
		}
	}

	@POST
	@Path("/topics/start")
	@Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_EDIT)
	@Operation(
			summary = "Start all services",
			description = "All services are (re)started",
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
	public Response startServices() {
		try {
			LoggingUtil.logRequestBegin(request);
			RulesService service = RulesService.getRulesService(servletContext);
			service.startService();
			return LoggingUtil.requestEnd(request, SuccessResponse.SUCCESS);
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(request, e);
		}
	}

	@POST
	@Path("/topics/stop")
	@Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_EDIT)
	@Operation(
			summary = "Stop all services",
			description = "All running services are stopped",
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
	public Response stopServices() {
		try {
			LoggingUtil.logRequestBegin(request);
			RulesService service = RulesService.getRulesService(servletContext);
			service.stopService();
			return LoggingUtil.requestEnd(request, SuccessResponse.SUCCESS);
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(request, e);
		}
	}

	@POST
	@Path("/topics/start/{inputtopic}")
	@Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_EDIT)
	@Operation(
			summary = "Start a named service",
			description = "(re)start a service for a specific topic",
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
	public Response startService(
			@PathParam("inputtopic")
			@Parameter(
					description = "name of input topic",
					example = "order_data"
					)
			String inputtopic) {
		try {
			LoggingUtil.logRequestBegin(request);
			RulesService service = RulesService.getRulesService(servletContext);
			service.startService(inputtopic);
			return LoggingUtil.requestEnd(request, SuccessResponse.SUCCESS);
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(request, e);
		}
	}

	@POST
	@Path("/topics/stop/{inputtopic}")
	@Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_EDIT)
	@Operation(
			summary = "Stop a named service",
			description = "Stop a service for a specific topic",
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
	public Response stopService(
			@PathParam("inputtopic")
			@Parameter(
					description = "name of input topic",
					example = "order_data"
					)
			String inputtopic) {
		try {
			LoggingUtil.logRequestBegin(request);
			RulesService service = RulesService.getRulesService(servletContext);
			service.stopService(inputtopic);
			return LoggingUtil.requestEnd(request, SuccessResponse.SUCCESS);
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(request, e);
		}
	}
}

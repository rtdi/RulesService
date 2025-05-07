package io.rtdi.bigdata.rulesservice;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.rtdi.bigdata.rulesservice.rest.ErrorResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;

public class LoggingUtil {
	public static final Logger LOGGER = LogManager.getLogger("io.rtdi.bigdata.rulesservice");

	public static void logRequestBegin(HttpServletRequest request) {
		LOGGER.info("Execution started: <{}> called by <{}>",
				request != null ? request.getRequestURI() : "?",
						request != null && request.getUserPrincipal() != null ? request.getUserPrincipal().getName() : "unknown");
	}

	public static void logRequestEnd(HttpServletRequest request) {
		LOGGER.info("Execution completed: <{}> called by <{}>",
				request != null ? request.getRequestURI() : "?",
						request != null && request.getUserPrincipal() != null ? request.getUserPrincipal().getName() : "unknown");
	}

	public static Response requestEnd(HttpServletRequest request, Object payload) {
		Response response = Response.ok(payload).build();
		logRequestEnd(request);
		return response;
	}

	public static Response requestEndTechnicalError(HttpServletRequest request, Exception e) {
		Response response = ErrorResponse.createErrorResponse(e);
		LOGGER.error("Exception", e);
		logRequestEnd(request);
		return response;
	}

	public static Response requestEndInputError(HttpServletRequest request, Exception e) {
		Response response = ErrorResponse.createErrorResponse(e, Status.BAD_REQUEST);
		LOGGER.warn("Exception", e);
		logRequestEnd(request);
		return response;
	}

	public static Response requestEndInputError(HttpServletRequest request, Exception e, Status status) {
		Response response = ErrorResponse.createErrorResponse(e, status);
		LOGGER.warn("Exception", e);
		logRequestEnd(request);
		return response;
	}

	public static void logRequestEndTechnicalError(HttpServletRequest request, Exception e) {
		LOGGER.error("Exception", e);
		logRequestEnd(request);
	}

	public static void logRequestEndInputError(HttpServletRequest request, Exception e) {
		LOGGER.warn("Exception", e);
		logRequestEnd(request);
	}

}

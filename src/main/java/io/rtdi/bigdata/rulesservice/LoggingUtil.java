package io.rtdi.bigdata.rulesservice;

import org.apache.logging.log4j.Logger;

import io.rtdi.bigdata.rulesservice.rest.ErrorResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;

public class LoggingUtil {

	public static void logRequestBegin(Logger log, HttpServletRequest request) {
		log.info("Execution started: <{}> called by <{}>",
				request != null ? request.getRequestURI() : "?",
						request != null && request.getUserPrincipal() != null ? request.getUserPrincipal().getName() : "unknown");
	}

	public static void logRequestEnd(Logger log, HttpServletRequest request) {
		log.info("Execution completed: <{}> called by <{}>",
				request != null ? request.getRequestURI() : "?",
						request != null && request.getUserPrincipal() != null ? request.getUserPrincipal().getName() : "unknown");
	}

	public static Response requestEnd(Logger log, HttpServletRequest request, Object payload) {
		Response response = Response.ok(payload).build();
		logRequestEnd(log, request);
		return response;
	}

	public static Response requestEndTechnicalError(Logger log, HttpServletRequest request, Exception e) {
		Response response = ErrorResponse.createErrorResponse(e);
		log.error("Exception", e);
		logRequestEnd(log, request);
		return response;
	}

	public static Response requestEndInputError(Logger log, HttpServletRequest request, Exception e) {
		Response response = ErrorResponse.createErrorResponse(e, Status.BAD_REQUEST);
		log.warn("Exception", e);
		logRequestEnd(log, request);
		return response;
	}

	public static Response requestEndInputError(Logger log, HttpServletRequest request, Exception e, Status status) {
		Response response = ErrorResponse.createErrorResponse(e, status);
		log.warn("Exception", e);
		logRequestEnd(log, request);
		return response;
	}

	public static void logRequestEndTechnicalError(Logger log, HttpServletRequest request, Exception e) {
		log.error("Exception", e);
		logRequestEnd(log, request);
	}

	public static void logRequestEndInputError(Logger log, HttpServletRequest request, Exception e) {
		log.warn("Exception", e);
		logRequestEnd(log, request);
	}

}

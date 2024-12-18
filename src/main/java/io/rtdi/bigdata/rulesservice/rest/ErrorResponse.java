package io.rtdi.bigdata.rulesservice.rest;

import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;

public class ErrorResponse {
	private String errormessage;

	public ErrorResponse(Exception e) {
		this.errormessage = e.getClass().getSimpleName() + ": " + e.getMessage();
	}

	public ErrorResponse(String message) {
		this.errormessage = message;
	}

	public String getErrormessage() {
		return errormessage;
	}

	public static Response createErrorResponse(Exception e) {
		return Response.serverError().entity(new ErrorResponse(e)).build();
	}

	public static Response createErrorResponse(Exception e, Status statuscode) {
		return Response.status(statuscode).entity(new ErrorResponse(e)).build();
	}


}

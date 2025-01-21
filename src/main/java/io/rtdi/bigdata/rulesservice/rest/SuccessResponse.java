package io.rtdi.bigdata.rulesservice.rest;

import jakarta.ws.rs.core.Response;

public class SuccessResponse {
	private String message;
	public static final SuccessResponse SUCCESS = new SuccessResponse("Success");
	public static final Response SUCCESS_RESPONSE = Response.ok(SUCCESS).build();
	
	public SuccessResponse(String message) {
		this.message = message;
	}

	public String getMessage() {
		return message;
	}
	
	public static Response createSuccessResponse(String message) {
		return Response.ok(new SuccessResponse(message)).build();
	}

	public static Response createSuccessResponse() {
		return SUCCESS_RESPONSE;
	}
}

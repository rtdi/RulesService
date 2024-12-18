package io.rtdi.bigdata.rulesservice;

import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.ws.rs.ext.ContextResolver;
import jakarta.ws.rs.ext.Provider;

@Provider
public class ObjectMapperContextResolver implements ContextResolver<ObjectMapper> {
	private final ObjectMapper MAPPER;

	public ObjectMapperContextResolver() {
		MAPPER = JsonMapper.builder()
				.enable(StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION)
				.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
				.addModule(new JavaTimeModule())
				.build();
	}

	@Override
	public ObjectMapper getContext(Class<?> type) {
		return MAPPER;
	}
}
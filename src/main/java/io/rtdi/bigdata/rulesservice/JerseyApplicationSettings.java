package io.rtdi.bigdata.rulesservice;

import java.nio.file.Path;

import org.glassfish.jersey.server.ResourceConfig;

import com.fasterxml.jackson.core.util.JacksonFeature;
import com.fasterxml.jackson.module.jakarta.xmlbind.JakartaXmlBindAnnotationModule;

import io.swagger.v3.jaxrs2.integration.resources.OpenApiResource;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.servers.Server;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.ServletContext;
import jakarta.ws.rs.ApplicationPath;


/**
 * Enable OpenAPI
 *
 */
@OpenAPIDefinition(
		info = @Info(
				title = "Rules Service",
				version = "1.0",
				description = "A Kafka KStream Rules Service"
				),
		tags = {
				@Tag(name = "Information", description = "APIs to interact with rules")
		},
		servers = {
				@Server(
						description = "Rule Service",
						url = "../../"
						)
		}
		)
@ApplicationPath("/protected/rest")
public class JerseyApplicationSettings extends ResourceConfig {

	public JerseyApplicationSettings() {
		super();
		packages("io.rtdi.bigdata.rulesservice");
		register(JacksonFeature.class);
		register(ObjectMapperContextResolver.class);
		register(RolesAllowedDynamicFeature2.class);
		register(new OpenApiResource());
		register(JakartaXmlBindAnnotationModule.class);
	}

	public static Path getWebAppRootPath(ServletContext servletcontext) {
		return Path.of(servletcontext.getRealPath("/"));
	}

}

package io.rtdi.bigdata.rulesservice.rest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineAPI;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaRegistryName;
import io.rtdi.bigdata.rulesservice.SchemaRuleSet;

public class SchemaListNameEntity {

	private List<SchemaName> schemas;
	
	public SchemaListNameEntity() {
	}

	public SchemaListNameEntity(IPipelineAPI<?,?,?,?> api, Collection<SchemaRuleSet> rules) throws IOException {
		schemas = new ArrayList<>();
		List<SchemaRegistryName> allschemas = api.getSchemas();
		Set<String> configuredschemas = new HashSet<>();
		if (rules != null) {
			for (SchemaRuleSet r : rules) {
				String name = r.getSchemaName().getName();
				configuredschemas.add(name);
			}
		}
		for (SchemaRegistryName schemaname : allschemas) {
			schemas.add(new SchemaName(schemaname.getName(), configuredschemas.contains(schemaname.getName())));
		}
	}

	public List<SchemaName> getSchemas() {
		return schemas;
	}

	public void setSchemas(List<SchemaName> schemas) {
		this.schemas = schemas;
	}
	
	public void addSchema(String name, boolean contained) {
		if (schemas == null) {
			schemas = new ArrayList<>();
		}
		schemas.add(new SchemaName(name, contained));
	}

	public static class SchemaName {
		String schemaname;
		private boolean configured = false;

		public SchemaName(String name, boolean configured) {
			this.schemaname = name;
			this.configured = configured;
		}

		public SchemaName() {
		}

		public String getSchemaname() {
			return schemaname;
		}

		public boolean isConfigured() {
			return configured;
		}
		
	}
}

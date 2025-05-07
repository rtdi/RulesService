package io.rtdi.bigdata.rulesservice.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ServiceSettings {
	private String properties;
	private boolean schemaregconnected;
	private int schemaregsubjects;
	private boolean kafkaconnected;
	private boolean servicerunning;
	private String errormessage;
	private boolean adminuser;
	private static final String propertieshelp = "The properties must contain info to connect to the schema registry and topics (read/write)";

	public String getProperties() {
		if (properties != null) {
			return properties;
		} else {
			return "basic.auth.credentials.source=USER_INFO\n"
					+ "schema.registry.basic.auth.user.info=<user>:<secret>\n"
					+ "schema.registry.url=https://<host1>,https://<host2>\n";
		}
	}
	public void setProperties(String properties) {
		this.properties = properties;
	}

	public boolean isSchemaregconnected() {
		return schemaregconnected;
	}
	public void setSchemaregconnected(boolean schemaregconnected) {
		this.schemaregconnected = schemaregconnected;
	}
	public boolean isKafkaconnected() {
		return kafkaconnected;
	}
	public void setKafkaconnected(boolean kafkaconnected) {
		this.kafkaconnected = kafkaconnected;
	}
	public boolean isServicerunning() {
		return servicerunning;
	}
	public void setServicerunning(boolean servicerunning) {
		this.servicerunning = servicerunning;
	}
	public String getErrormessage() {
		return errormessage;
	}
	public void setErrormessage(String errormessage) {
		this.errormessage = errormessage;
	}
	public boolean isAdminuser() {
		return adminuser;
	}
	public void setAdminuser(boolean adminuser) {
		this.adminuser = adminuser;
	}
	public int getSchemaregsubjects() {
		return schemaregsubjects;
	}
	public void setSchemaregsubjects(int schemaregsubjects) {
		this.schemaregsubjects = schemaregsubjects;
	}
	public String getPropertieshelp() {
		return propertieshelp;
	}

}

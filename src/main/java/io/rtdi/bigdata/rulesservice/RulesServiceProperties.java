package io.rtdi.bigdata.rulesservice;

import java.io.File;
import java.util.List;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ServiceProperties;
import io.rtdi.bigdata.connector.properties.atomic.PropertyRoot;

public class RulesServiceProperties extends ServiceProperties<RuleStep> {

	public RulesServiceProperties(String name) throws PropertiesException {
		super(name);
	}

	public RulesServiceProperties(File dir, String name) throws PropertiesException {
		super(dir, name);
	}

	public RuleStep createRuleStep(String name) {
		RuleStep microservice = new RuleStep(name);
		addMicroService(microservice);
		return microservice;
	}

	@Override
	public void setValue(PropertyRoot pg, List<String> services) throws PropertiesException {
		super.setValue(pg, services);
		if (services != null) {
			for (String s : services) {
				createRuleStep(s);
			}
		}
	}

	@Override
	public void read(File directory) throws PropertiesException {
		super.read(directory);
	}

	@Override
	public void write(File directory) throws PropertiesException {
		super.write(directory);
	}

	@Override
	protected RuleStep readMicroservice(File dir) throws PropertiesException {
		RuleStep m = new RuleStep(dir.getName());
		for (File schemadir : dir.listFiles()) {
			if (schemadir.isDirectory()) {
				SchemaRuleSet s = new SchemaRuleSet(schemadir);
				m.addSchemaRuleSet(s);
			}
		}
		return m;
	}

	@Override
	protected void writeMicroservice(RuleStep m, File dir) throws PropertiesException {
		for (SchemaRuleSet s : m.getSchemaRules().values()) {
			File schemadir = new File(dir.getAbsolutePath() + File.separatorChar + s.getSchemaname());
			if (!schemadir.exists()) {
				schemadir.mkdir();
			}
			s.write(schemadir);
		}
	}

}

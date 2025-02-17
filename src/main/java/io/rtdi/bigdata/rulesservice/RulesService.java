package io.rtdi.bigdata.rulesservice;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.rtdi.appcontainer.utils.UsageStatisticSender;
import io.rtdi.bigdata.rulesservice.config.RuleFileDefinition;
import io.rtdi.bigdata.rulesservice.config.RuleFileName;
import io.rtdi.bigdata.rulesservice.config.RuleStep;
import io.rtdi.bigdata.rulesservice.config.ServiceSettings;
import io.rtdi.bigdata.rulesservice.config.ServiceStatus;
import io.rtdi.bigdata.rulesservice.config.TopicRule;
import io.rtdi.bigdata.rulesservice.jexl.JexlRecord;
import io.rtdi.bigdata.rulesservice.rest.SampleData;
import io.rtdi.bigdata.rulesservice.rules.RuleUtils;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletContextEvent;
import jakarta.servlet.ServletContextListener;
import jakarta.servlet.annotation.WebListener;

/**
 * Main java class running the rules service
 *
 */
@WebListener
public class RulesService implements ServletContextListener {
	protected final Logger log = LogManager.getLogger(this.getClass().getName());

	private ScheduledExecutorService executor;
	private CachedSchemaRegistryClient schemaclient;
	private ServletContext context;
	private Path settingsdir;
	private Path rulefiledir;
	private Exception globalexception;
	private String properties;
	private Map<String, String> propertiesmap;
	private Admin admin;
	/**
	 * Map<Inputtopic, Map<Instance Number, RuleFileKStream>>
	 */
	private Map<String, RuleFileKStream> services = new HashMap<>();

	/**
	 * Default constructor.
	 */
	public RulesService() {
		super();
	}

	public Map<String, String> getKafkaProperties() {
		return propertiesmap;
	}

	public String getBootstrapServers() {
		if (propertiesmap != null) {
			return propertiesmap.get("bootstrap.servers");
		} else {
			return null;
		}
	}

	public long getRowsProduced() {
		if (services != null) {
			long count = 0L;
			for (RuleFileKStream service : services.values()) {
				count += service.getRowsprocessed();
			}
			return count;
		} else {
			return 0L;
		}
	}

	public String getState() {
		if (services != null && services.size() > 0) {
			return "RUNNING";
		} else {
			return "IDLE";
		}
	}

	public Long getLastDataTimestamp() {
		if (services != null) {
			long ts = 0L;
			for (RuleFileKStream service : services.values()) {
				if (ts < service.getLastprocessedtimestamp()) {
					ts = service.getLastprocessedtimestamp();
				}
			}
			return ts;
		} else {
			return null;
		}
	}

	/**
	 * @see ServletContextListener#contextDestroyed(ServletContextEvent)
	 */
	@Override
	public void contextDestroyed(ServletContextEvent sce) {
		if (executor != null) {
			executor.shutdownNow();
			try {
				executor.awaitTermination(20, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
			}
		}
		close();
	}

	/**
	 * @see ServletContextListener#contextInitialized(ServletContextEvent)
	 */
	@Override
	public void contextInitialized(ServletContextEvent sce) {
		if (System.getenv("STATISTICS") == null
				|| !System.getenv("STATISTICS").equalsIgnoreCase("FALSE")) {
			this.executor = Executors.newSingleThreadScheduledExecutor();
			executor.scheduleAtFixedRate(new UsageStatisticSender(this), 1, 10, TimeUnit.MINUTES);
			this.context = sce.getServletContext();
			this.context.setAttribute("service", this);
		}
		try {
			Context initCtx = new InitialContext();
			Context envCtx = (Context) initCtx.lookup("java:comp/env");
			try {
				Object o = envCtx.lookup("rulesettings");
				if (o != null) {
					settingsdir = Path.of(o.toString());
					log.info("Found JNDI resource name <java:comp/env/rulesettings>, hence the settings directory is <{}>", settingsdir);
				}
			} catch (NamingException e) {
				log.info("No JNDI resource found in the context.xml for name <java:comp/env/rulesettings>, hence using the default");
			}
			try {
				Object o = envCtx.lookup("rulegroups");
				if (o != null) {
					rulefiledir = Path.of(o.toString());
					log.info("Found JNDI resource name <java:comp/env/rulegroups>, hence the rules root directory is <{}>", rulefiledir);
				}
			} catch (NamingException e) {
				log.info("No JNDI resource found in the context.xml for name <java:comp/env/rulegroups>, hence the default");
			}
		} catch (Exception e) {
			this.globalexception = e;
			log.info("Exception when reading the webserver settings", e);
		}
		try {
			if (settingsdir == null) {
				settingsdir = Path.of("/apps/rulesservice/settings");
				log.info("Settings directory is the default <{}>", settingsdir);
			}
			if (rulefiledir == null) {
				rulefiledir = Path.of("/apps/rulesservice/definitions");
				log.info("Rulefile directory is the default <{}>", rulefiledir);
			}
			configure();
			startService();
		} catch (Exception e) {
			this.globalexception = e;
			log.info("Exception when reading the webserver settings", e);
		}
	}

	public void startService() throws IOException {
		stopService();
		Map<String, TopicRule> topicrulefiles = getTopicRuleFiles();
		for (Entry<String, TopicRule> entry : topicrulefiles.entrySet()) {
			TopicRule r = entry.getValue();
			RuleFileKStream rule = new RuleFileKStream(r.getRulefiles(), this, r.getInputtopicname(), r.getOutputtopicname(), r.getInstances());
			if (rule.getRulefiledefinitions().size() > 0) {
				services.put(r.getInputtopicname(), rule);
				rule.start();
				log.info("Thread for topic {} started", r.getInputtopicname());
			} else {
				log.info("Topic {} has no active rules", r.getInputtopicname());
			}
		}
	}

	public void startService(String topicname) throws IOException, InterruptedException {
		stopService(topicname);
		Map<String, TopicRule> topicrules = getTopicRuleFiles();
		TopicRule r = topicrules.get(topicname);
		if (r != null) {
			RuleFileKStream rule = new RuleFileKStream(r.getRulefiles(), this, r.getInputtopicname(), r.getOutputtopicname(), r.getInstances());
			if (rule.getRulefiledefinitions().size() > 0) {
				services.put(r.getInputtopicname(), rule);
				rule.start();
				log.info("Thread for topic {} started", r.getInputtopicname());
			} else {
				log.info("Topic {} has no active rules", r.getInputtopicname());
			}
		}
	}

	public void startFailedServices() throws IOException {
		for (Entry<String, RuleFileKStream> entry : services.entrySet()) {
			RuleFileKStream stream = entry.getValue();
			if (! stream.isAlive()) {
				RuleFileKStream rule = new RuleFileKStream(stream.getRulefiles(), this, stream.getInputtopicname(), stream.getOutputtopicname(), stream.getInstances());
				if (rule.getRulefiledefinitions().size() > 0) {
					services.put(rule.getInputtopicname(), rule);
					rule.start();
					log.info("Thread for topic {} re-started", rule.getInputtopicname());
				} else {
					log.info("Topic {} has no active rules", rule.getInputtopicname());
				}
			}
		}
	}

	public ServiceStatus getServiceStatus() throws IOException {
		return new ServiceStatus(this);
	}

	public void stopService() throws IOException {
		/*
		 * Signal all to stop
		 */
		for (RuleFileKStream service : services.values()) {
			service.interrupt();
		}
		/*
		 * Remove all stopped instances within 1 minute
		 */
		long until = System.currentTimeMillis() + 60000L;
		while (System.currentTimeMillis() < until && services.size() > 0) {
			Iterator<Entry<String, RuleFileKStream>> iter = services.entrySet().iterator();
			while (iter.hasNext()) {
				Entry<String, RuleFileKStream> service = iter.next();
				try {
					if (service.getValue().join(Duration.ofSeconds(2))) {
						iter.remove();
						log.info("Thread for topic {} stopped", service.getKey());
					}
				} catch (InterruptedException e) {
					// NOOP as we have to close all resources, no matter what
				}
			}
		}
	}

	public void stopService(String topicname) throws IOException, InterruptedException {
		RuleFileKStream service = services.get(topicname);
		if (service != null) {
			/*
			 * Signal all to stop
			 */
			service.interrupt();
			/*
			 * Remove within 1 minute
			 */
			if (service.join(Duration.ofSeconds(60))) {
				services.remove(topicname);
				log.info("Thread for topic {} stopped", topicname);
			}
		}
	}

	public static RulesService getRulesService(ServletContext context) {
		return (RulesService) context.getAttribute("service");
	}

	private void configure() throws IOException, RestClientException {
		this.globalexception = null;
		File propertiesfile = new File(settingsdir.toFile(), "kafka.properties");
		if (!propertiesfile.isFile()) {
			log.error("The mandatory kafka.properties file does not exist at <{}>", propertiesfile.toString());
			throw new IOException("The mandatory kafka.properties file does not exist at <" + propertiesfile.toString() + ">");
		} else {
			log.info("Found the kafka.properties file as <{}>", propertiesfile.toString());
		}
		Properties kafkaproperties = new Properties();
		try (InputStream is = new FileInputStream(propertiesfile)) {
			kafkaproperties.load(is);
		}
		this.properties = Files.readString(settingsdir.resolve("kafka.properties"), StandardCharsets.UTF_8);
		String schemaurls = kafkaproperties.getProperty("schema.registry.url");
		if (schemaurls == null) {
			throw new IOException("The kafka.properties file does not contain a <schema.registry.url>");
		}
		String[] urls = schemaurls.split(",");
		List<String> schemaRegistryUrls = new ArrayList<>();
		for (String url : urls) {
			schemaRegistryUrls.add(url.trim());
		}
		propertiesmap = kafkaproperties.entrySet().stream().collect(
				Collectors.toMap(
						e -> e.getKey().toString(),
						e -> e.getValue().toString()
						)
				);
		schemaclient = new CachedSchemaRegistryClient(schemaRegistryUrls, 100, propertiesmap);
		schemaclient.getMode(); // invoke schema registry to validate connection
		admin = Admin.create(kafkaproperties);
		admin.describeCluster();
	}

	public List<RuleFileName> getRuleFiles(String schemaname) {
		List<RuleFileName> groups = RuleFileDefinition.getAllRuleFiles(rulefiledir, schemaname);
		return groups;
	}

	public List<RuleFileName> getAllRuleFiles() {
		List<RuleFileName> groups = RuleFileDefinition.getAllRuleFiles(rulefiledir);
		return groups;
	}


	public Collection<String> getSubjects() throws IOException, RestClientException {
		Collection<String> subjects = schemaclient.getAllSubjects();
		return subjects;
	}

	public Schema getLatestSchema(String subjectname) throws IOException, RestClientException {
		SchemaMetadata metadata = schemaclient.getLatestSchemaMetadata(subjectname);
		String schemastring = metadata.getSchema();
		org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
		org.apache.avro.Schema schema = parser.parse(schemastring);
		return schema;
	}

	public Exception getGlobalException() {
		return globalexception;
	}

	public Path getRuleFileRootDir() {
		return this.rulefiledir;
	}

	public ServiceSettings getConfig(boolean isadmin) {
		ServiceSettings settings = new ServiceSettings();
		settings.setAdminuser(isadmin);
		if (isadmin) {
			// The call has been made by someone with admin permissions
			settings.setProperties(properties);
		} else {
			settings.setProperties("# Data not exposed; only users with the role <rulesadmin> can view/edit");
		}
		settings.setSchemaregconnected(false);
		if (schemaclient != null) {
			try {
				Collection<String> schemas = getSubjects();
				settings.setSchemaregsubjects(schemas.size());
				settings.setSchemaregconnected(true);
			} catch (IOException | RestClientException e) {
				log.error("getConfig() ran into an error", e);
				globalexception = e;
			}
		}
		settings.setKafkaconnected(false);
		if (admin != null) {
			try {
				DescribeClusterResult result = admin.describeCluster();
				result.nodes().get(2, TimeUnit.SECONDS);
				settings.setKafkaconnected(true);
			} catch (Exception e) {
				log.error("getConfig() ran into an error", e);
				globalexception = e;
			}
		}
		if (services.size() > 0) {
			settings.setServicerunning(true);
		} else {
			settings.setServicerunning(false);
		}
		settings.setErrormessage(globalexception != null ? globalexception.getMessage() : null);
		return settings;
	}

	public Collection<TopicRule> getTopicsAndRules() throws IOException {
		ListTopicsResult topicsresult = admin.listTopics();
		KafkaFuture<Set<String>> future = topicsresult.names();
		Map<String, TopicRule> fileset = getTopicRuleFiles();
		Collection<String> l;
		try {
			l = future.get(20, TimeUnit.SECONDS);
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			throw new IOException("Failed to read the list of topics from Kafka within 20 seconds", e);
		}
		TreeSet<TopicRule> out = new TreeSet<>();
		for (String topicname : l) {
			TopicRule tr = fileset.get(topicname);
			if (tr != null) {
				out.add(tr);
			} else {
				out.add(new TopicRule(topicname));
			}
		}
		return out;
	}

	/**
	 * @return a map<inputtopicname, TopicRule> data
	 * @throws IOException
	 */
	public Map<String, TopicRule> getTopicRuleFiles() throws IOException {
		ObjectMapper om = new ObjectMapper();
		Path topicrules = rulefiledir.resolve("topics");
		File topicruledir = topicrules.toFile();
		Map<String, TopicRule> fileset = new HashMap<>();
		if (topicruledir.isDirectory()) {
			File[] files = topicruledir.listFiles();
			for(File f : files) {
				if (f.isFile() && f.getName().endsWith(".json")) {
					TopicRule tr = om.readValue(f, TopicRule.class);
					fileset.put(f.getName().substring(0, f.getName().length()-5), tr);
				}
			}
		}
		return fileset;
	}


	public List<TopicRule> saveTopicRules(List<TopicRule> input) {
		Path topicruledir = rulefiledir.resolve("topics");
		File f = topicruledir.toFile();
		if (!f.exists()) {
			f.mkdirs();
		}
		for ( TopicRule topicrule : input) {
			if (topicrule.getModified() != null && topicrule.getModified() == Boolean.TRUE) {
				if (topicrule.getInputtopicname() == null) {
					// ignore
				} else if (topicrule.getOutputtopicname() == null) {
					topicrule.delete(topicruledir);
					topicrule.setModified(null);
					topicrule.setInfo("deleted");
				} else if (topicrule.getInputtopicname().equals(topicrule.getOutputtopicname())) {
					topicrule.setInfo("The rule for topic <" + topicrule.getInputtopicname() + "> cannot write into itself, output topic must be different");
				} else {
					try {
						topicrule.save(topicruledir);
						topicrule.setModified(null);
						topicrule.setInfo("saved");
					} catch (IOException e) {
						log.error("Failed to save the file for topic <{}>", topicrule.getInputtopicname(), e);
						topicrule.setInfo("Failed to save the topic file (" + e.getMessage() + ")");
					}
					try {
						startService(topicrule.getInputtopicname());
					} catch (IOException | InterruptedException e) {
						log.error("Failed to start the service for the topic <{}>", topicrule.getInputtopicname(), e);
						topicrule.setInfo("Failed to start the service for the topic <" + topicrule.getInputtopicname() + ">: " + e.getMessage());
					}
				}
			} else {
				topicrule.setInfo(null);
			}
		}
		return input;
	}

	public void saveConfig(ServiceSettings input) throws IOException {
		String properties = input.getProperties();
		if (!settingsdir.toFile().exists()) {
			settingsdir.toFile().mkdirs();
		}
		Files.writeString(settingsdir.resolve("kafka.properties"), properties, StandardCharsets.UTF_8);
		close();
		try {
			configure();
		} catch (Exception e) {
			this.globalexception = e;
		}
	}

	private void close() {
		log.info("Closing all resources");
		try {
			stopService();
		} catch (IOException e) {
			log.error("Stopping the services failed - ignored", e);
		}
		if (admin != null) {
			try {
				admin.close(Duration.ofSeconds(10));
			} catch (Exception e) {
				log.error("Closing the Kafka admin client within 10 seconds failed - ignored", e);
			}
		}
		if (schemaclient != null) {
			try {
				schemaclient.close();
			} catch (Exception e) {
				log.error("Closing the schema registry client failed - ignored", e);
			}
		}
	}

	public Collection<String> getTopics() throws IOException {
		ListTopicsResult topicsresult = admin.listTopics();
		KafkaFuture<Set<String>> future = topicsresult.names();
		try {
			return future.get(20, TimeUnit.SECONDS);
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			throw new IOException("Failed to read the list of topics from Kafka within 20 seconds", e);
		}
	}

	public RuleStep applySampleData(RuleFileDefinition input, Integer stepindex) throws IOException {
		if (input.getSchema() == null) {
			throw new PropertiesException("The Rule file does not contain the schema information");
		}
		Parser parser = new Schema.Parser();
		Schema schema = parser.parse(input.getSchema());
		RuleStep step = input.getRulesteps().get(stepindex);
		JexlRecord rec = (JexlRecord) RuleUtils.getSampleValue(step, schema);
		step.apply(rec, rec, true);
		rec.mergeReplacementValues();
		step.updateSampleOutput(rec);
		return step;
	}

	public RuleFileDefinition applySampleFile(RuleFileDefinition input) throws IOException {
		if (input.getSchema() == null) {
			throw new PropertiesException("The Rule file does not contain the schema information");
		}
		if (input.getSamplefile() == null) {
			throw new PropertiesException("The Rule file does not specify a sample file");
		}
		Parser parser = new Schema.Parser();
		Schema schema = parser.parse(input.getSchema());
		SampleData sample = SampleData.load(getRuleFileRootDir(), input.getInputsubjectname(), Path.of(input.getSamplefile()));
		JexlRecord samplerecord = sample.createRecord(schema);
		input.apply(samplerecord, true);
		return input;
	}

	public CachedSchemaRegistryClient getSchemaclient() {
		return schemaclient;
	}

	public Map<String, RuleFileKStream> getServices() {
		return services;
	}

}

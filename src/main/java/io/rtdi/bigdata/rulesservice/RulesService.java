package io.rtdi.bigdata.rulesservice;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Parser;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.rtdi.appcontainer.utils.UsageStatisticSender;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.AvroJexlContext;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlArray;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlRecord;
import io.rtdi.bigdata.kafka.avro.AvroDataTypeException;
import io.rtdi.bigdata.kafka.avro.AvroUtils;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroType;
import io.rtdi.bigdata.kafka.avro.datatypes.IAvroDatatype;
import io.rtdi.bigdata.kafka.avro.recordbuilders.SchemaBuilder;
import io.rtdi.bigdata.kafka.avro.recordbuilders.ValueSchema;
import io.rtdi.bigdata.rulesservice.config.RuleFileName;
import io.rtdi.bigdata.rulesservice.config.ServiceSettings;
import io.rtdi.bigdata.rulesservice.config.TopicRule;
import io.rtdi.bigdata.rulesservice.definition.RuleFileDefinition;
import io.rtdi.bigdata.rulesservice.definition.RuleStep;
import io.rtdi.bigdata.rulesservice.rest.SampleData;
import io.rtdi.bigdata.rulesservice.rules.ArrayRule;
import io.rtdi.bigdata.rulesservice.rules.RecordRule;
import io.rtdi.bigdata.rulesservice.rules.Rule;
import io.rtdi.bigdata.rulesservice.rules.UnionRule;
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
	private Path rulegroupdir;
	private Exception globalexception;
	private String properties;
	private Map<String, String> propertiesmap;
	private Admin admin;

	/**
	 * Default constructor.
	 */
	public RulesService() {
		super();
	}

	public void start() throws FileNotFoundException, IOException {
		/*
		 * Two topics can use the same schema, yet have different rules.
		 * One topic can have multiple schemas.
		 * Rules consists of multiple rule steps to build rules on top of previous rules
		 *
		 * Topic = Map<Schema, RuleGroup<Schema>>               One topic has for each schema one RuleGroup
		 * RuleGroup<Schema> = List<RuleStep<OutputSchema>>     One RuleGroup is for a single schema and consists of multiple RuleSteps
		 * RuleStep<OutputSchema>                               One RuleStep is an intermediate transformation; Note that the output if a rule step can have more fields than the topic schema
		 *
		 */
		StreamsBuilder builder = new StreamsBuilder();
		KStream<Bytes, Bytes> transformation = builder.stream(
				"input-topic",
				Consumed.with(Serdes.Bytes(), Serdes.Bytes()));
		KStream<Bytes, Bytes> result = transformation.mapValues(value -> value);
		result.to("output-topic", Produced.with(Serdes.Bytes(), Serdes.Bytes()));
		StreamsConfig streamsConfig = new StreamsConfig(propertiesmap);
		KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig);
		streams.start();
		streams.close();
	}

	public Map<String, String> getKafkaProperties() {
		return propertiesmap;
	}

	public String getBootstrapServers() {
		return null;
	}

	public long getRowsProduced() {
		return 0;
	}

	public String getState() {
		return null;
	}

	public Long getLastDataTimestamp() {
		return null;
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
		if (System.getenv("APPCONTAINERSTATISTICS") == null
				|| !System.getenv("APPCONTAINERSTATISTICS").equalsIgnoreCase("FALSE")) {
			this.executor = Executors.newSingleThreadScheduledExecutor();
			executor.scheduleAtFixedRate(new UsageStatisticSender(this), 1, 10, TimeUnit.MINUTES);
			this.context = sce.getServletContext();
			this.context.setAttribute("service", this);
		}
		try {
			Context initCtx = new InitialContext();
			Context envCtx = (Context) initCtx.lookup("java:comp/env");
			Object o = envCtx.lookup("rulesettings");
			if (o != null) {
				settingsdir = Path.of(o.toString());
				log.info("Found JNDI resource name <java:comp/env/rulesettings>, hence the settings directory is <{}>", settingsdir);
			} else {
				settingsdir = Path.of("/apps/rulesservice/settings");
				log.info("No JNDI resource found for name <java:comp/env/rulesettings>, hence the settings directory is the default <{}>", settingsdir);
			}
			o = envCtx.lookup("rulegroups");
			if (o != null) {
				rulegroupdir = Path.of(o.toString());
				log.info("Found JNDI resource name <java:comp/env/rulegroups>, hence the rules root directory is <{}>", rulegroupdir);
			} else {
				rulegroupdir = Path.of("/apps/rulesservice/definitions");
				log.info("No JNDI resource found for name <java:comp/env/rulegroups>, hence the rules root directory is the default <{}>", rulegroupdir);
			}
			configure();
		} catch (Exception e) {
			this.globalexception = e;
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
		List<RuleFileName> groups = RuleFileDefinition.getAllRuleFiles(rulegroupdir, schemaname);
		return groups;
	}

	public List<RuleFileName> getAllRuleGroups() {
		List<RuleFileName> groups = RuleFileDefinition.getAllRuleFiles(rulegroupdir);
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
		return this.rulegroupdir;
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
				admin.describeCluster();
				settings.setKafkaconnected(true);
			} catch (Exception e) {
				log.error("getConfig() ran into an error", e);
				globalexception = e;
			}
		}
		settings.setErrormessage(globalexception != null ? globalexception.getMessage() : null);
		return settings;
	}

	public Collection<TopicRule> getTopicRules() throws IOException {
		ObjectMapper om = new ObjectMapper();
		ListTopicsResult topicsresult = admin.listTopics();
		KafkaFuture<Set<String>> future = topicsresult.names();
		Path topicrules = rulegroupdir.resolve("topics");
		File topicruledir = topicrules.toFile();
		Map<String, File> fileset = new HashMap<>();
		if (topicruledir.isDirectory()) {
			File[] files = topicruledir.listFiles();
			for(File f : files) {
				if (f.isFile() && f.getName().endsWith(".json")) {
					fileset.put(f.getName().substring(0, f.getName().length()-5), f);
				}
			}
		}
		Collection<String> l;
		try {
			l = future.get(20, TimeUnit.SECONDS);
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			throw new IOException("Failed to read the list of topics from Kafka within 20 seconds", e);
		}
		TreeSet<TopicRule> out = new TreeSet<>();
		for (String topicname : l) {
			File f = fileset.get(topicname);
			TopicRule tr;
			if (f != null) {
				tr = om.readValue(f, TopicRule.class);
			} else {
				tr = new TopicRule(topicname);
			}
			out.add(tr);
		}
		return out;
	}

	public List<TopicRule> saveTopicRules(List<TopicRule> input) {
		Path topicruledir = rulegroupdir.resolve("topics");
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
						topicrule.setInfo("Failed to save the topic file (" + e.getMessage() + ")");
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

	public void createCleansedSchema(String subjectname, String outputsubjectname) throws IOException, RestClientException {
		SchemaMetadata metadata = schemaclient.getLatestSchemaMetadata(subjectname);
		// check if we know about that particular id already
		String schemastring = metadata.getSchema();
		org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
		org.apache.avro.Schema schema = parser.parse(schemastring);
		// check if it has the _audit field already
		if (schema.getField(ValueSchema.AUDIT) == null) {
			// if no, derive the new schema
			SchemaBuilder outputschemabuilder = new SchemaBuilder(schema.getName() + "_CLEANSED", schema, false);
			ValueSchema.addAuditField(outputschemabuilder);
			outputschemabuilder.build();
			schema = outputschemabuilder.getSchema();
			AvroSchemaProvider provider = new AvroSchemaProvider();
			io.confluent.kafka.schemaregistry.client.rest.entities.Schema schemaentity = new io.confluent.kafka.schemaregistry.client.rest.entities.Schema(
					outputsubjectname, 0, 0, new SchemaString(schema.toString()));
			ParsedSchema parsed = provider.parseSchemaOrElseThrow(schemaentity , false, false);
			// register the schema - if already known the API call just returns the schema id
			schemaclient.register(outputsubjectname, parsed);
		}
	}

	public RuleStep applySampleData(RuleFileDefinition input, Integer stepindex) throws IOException {
		if (input.getSchema() == null) {
			throw new PropertiesException("The Rule file does not contain the schema information");
		}
		Parser parser = new Schema.Parser();
		Schema schema = parser.parse(input.getSchema());
		RuleStep step = input.getRulesteps().get(stepindex);
		JexlRecord rec = (JexlRecord) getSampleValue(step, schema);
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

	private static Object getSampleValue(Rule rule, Schema schema) throws PropertiesException {
		if (schema != null) {
			switch (schema.getType()) {
			case ARRAY:
				if (rule instanceof ArrayRule ar) {
					if (ar.getRules() != null && ar.getRules().size() > 0) {
						JexlArray<Object> a = new JexlArray<>(schema, null);
						Object value = getSampleValue(rule.getRules().get(0), schema.getElementType());
						if (value != null) {
							a.add(value);
						}
						return a;
					}
				} else {
					throw new PropertiesException("There is a mismatch between the schema and rule type for the field <" + rule.getFieldname() + ">");
				}
				break;
			case RECORD:
				if (rule instanceof RecordRule) {
					if (rule.getRules() != null) {
						JexlRecord rec = new JexlRecord(schema, null);
						for ( Rule r : rule.getRules()) {
							if (r.getFieldname() != null) {
								Field field = schema.getField(r.getFieldname());
								if (field != null) {
									Schema fieldschema = AvroUtils.getBaseSchema(field.schema());
									rec.put(r.getFieldname(), getSampleValue(r, fieldschema));
								}
							}
						}
						return rec;
					}
				} else {
					throw new PropertiesException("There is a mismatch between the schema and rule type for the field <" + rule.getFieldname() + ">");
				}
				break;
			case UNION:
				if (rule instanceof UnionRule) {
					if (rule.getRules() != null) {
						for ( Rule r : rule.getRules()) {
							if (r.getSampleinput() != null) {
								return getSampleValue(r, findUnionSchema(rule.getSchemaname(), schema.getTypes()));
							}
						}
					}
				}
				break;
			case BOOLEAN:
			case BYTES:
			case DOUBLE:
			case ENUM:
			case FIXED:
			case FLOAT:
			case INT:
			case LONG:
			case MAP:
			case NULL:
			case STRING:
				IAvroDatatype dt = AvroType.getAvroDataType(schema);
				if (dt != null) {
					return dt.convertToInternal(rule.getSampleinput());
				}
				break;
			default:
				break;
			}
		}
		return null;
	}

	private static Schema findUnionSchema(String schemaname, List<Schema> list) {
		for (Schema s : list) {
			if (s.getFullName().equals(schemaname)) {
				return s;
			}
		}
		return null;
	}

	public CachedSchemaRegistryClient getSchemaclient() {
		return schemaclient;
	}

}

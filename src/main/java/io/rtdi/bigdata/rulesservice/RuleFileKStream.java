package io.rtdi.bigdata.rulesservice;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.Schema;
import org.apache.kafka.common.KafkaException;
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

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.rtdi.bigdata.kafka.avro.AvroDeserializer;
import io.rtdi.bigdata.kafka.avro.AvroSerializer;
import io.rtdi.bigdata.kafka.avro.recordbuilders.SchemaBuilder;
import io.rtdi.bigdata.kafka.avro.recordbuilders.ValueSchema;
import io.rtdi.bigdata.rulesservice.config.RuleFileDefinition;
import io.rtdi.bigdata.rulesservice.jexl.JexlAvroDeserializer;
import io.rtdi.bigdata.rulesservice.jexl.JexlRecord;

public class RuleFileKStream extends Thread {

	protected final Logger log = LogManager.getLogger(this.getClass().getName());

	private Map<String, RuleFileDefinition> ruledefinitions = new HashMap<>();
	private Map<Integer, Map.Entry<Integer, Schema>> inoutschema = new HashMap<>();
	private RulesService service;
	private String inputtopicname;
	private String outputtopicname;
	private AtomicLong rowsprocessed = new AtomicLong();
	private volatile long lastprocessedtimestamp = 0L;
	private AvroSchemaProvider provider = new AvroSchemaProvider();
	private Exception error;
	private Integer instances;
	private List<String> rulefiles;
	private AtomicLong processingtime = new AtomicLong();


	public RuleFileKStream(List<String> rulefiles, RulesService service, String inputtopicname, String outputtopicname, Integer instances) throws IOException {
		this.service = service;
		this.inputtopicname = inputtopicname;
		this.outputtopicname = outputtopicname;
		if (instances == null) {
			this.instances = 1;
		} else {
			this.instances = instances;
		}
		this.rulefiles = rulefiles;
		for (String location : rulefiles) {
			int pos = location.indexOf('/');
			if (pos <= 0) {
				throw new IOException("Rulefile path does not follow the convention <subject>/<path>");
			}
			String subjectname = location.substring(0, pos);
			String path = location.substring(pos+1);
			RuleFileDefinition rule = RuleFileDefinition.load(service.getRuleFileRootDir(), subjectname, Path.of(path), true);
			if (rule != null) {
				this.ruledefinitions.put(rule.schema().getFullName(), rule);
			}
		}
	}

	@Override
	public void run() {
		if (ruledefinitions.size() == 0) {
			this.error = new IOException("Stream has no active rule files");
		} else {
			StreamsBuilder builder = new StreamsBuilder();
			KStream<Bytes, Bytes> transformation = builder.stream(
					inputtopicname,
					Consumed.with(Serdes.Bytes(), Serdes.Bytes()));
			KStream<Bytes, Bytes> result = transformation.mapValues(value -> mapper(value));
			result.to(outputtopicname, Produced.with(Serdes.Bytes(), Serdes.Bytes()));
			HashMap<String, String> map = new HashMap<>(service.getKafkaProperties());
			map.put(StreamsConfig.APPLICATION_ID_CONFIG, "rulesservice_" + inputtopicname);
			map.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, String.valueOf(instances));
			StreamsConfig streamsConfig = new StreamsConfig(map);
			try (KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig);) {
				streams.start();
				try {
					while (streams.state().isRunningOrRebalancing()) {
						Thread.sleep(Duration.ofMinutes(1));
					}
				} catch (InterruptedException e) {
					// exit loop and stop thread
				}
			} catch (Exception e) {
				this.error = e;
			}
		}
	}

	private Bytes mapper(Bytes value) {
		long startts = System.currentTimeMillis();
		byte[] data = value.get();

		int schemaid;
		try {
			schemaid = AvroDeserializer.getSchemaId(data);
		} catch (IOException e) {
			log.error("This input is not a valid Avro record with a magic byte", e);
			throw new KafkaException("This input is not a valid Avro record with a magic byte", e);
		}

		ParsedSchema writerparsedschema;
		try {
			writerparsedschema = service.getSchemaclient().getSchemaById(schemaid);
		} catch (IOException | RestClientException e) {
			log.error("Exception when reading the schema <{}> from the schema registry by id", schemaid, e);
			throw new KafkaException("Exception when reading the schema <" + schemaid + "> from the schema registry by id", e);
		}
		Schema writerschema = (Schema) writerparsedschema.rawSchema();

		RuleFileDefinition ruledefinition = ruledefinitions.get(writerschema.getFullName());
		if (ruledefinition == null) {
			/*
			 * No transformations for that schema, hence a pass-through
			 */
			return value;
		} else {
			JexlRecord inputrecord;
			if (writerschema.getField(ValueSchema.AUDIT) == null) {
				synchronized (this) {
					Entry<Integer, Schema> schemadata = inoutschema.get(schemaid);
					if (schemadata == null) {
						// if no, derive the new schema
						SchemaBuilder outputschemabuilder = new SchemaBuilder(writerschema.getName() + "_CLEANSED", writerschema, false);
						ValueSchema.addAuditField(outputschemabuilder);
						outputschemabuilder.build();
						Schema outputschema = outputschemabuilder.getSchema();
						io.confluent.kafka.schemaregistry.client.rest.entities.Schema schemaentity = new io.confluent.kafka.schemaregistry.client.rest.entities.Schema(
								ruledefinition.getOutputsubjectname(), 0, 0, new SchemaString(outputschema.toString()));
						ParsedSchema parsed = provider.parseSchemaOrElseThrow(schemaentity , false, false);
						// register the schema - if already known the API call will not create it and just return the schema id
						int outputschemaid;
						try {
							outputschemaid = service.getSchemaclient().register(ruledefinition.getOutputsubjectname(), parsed);
						} catch (IOException | RestClientException e) {
							log.error("Exception when registering the schema for subject name <{}>\nschema: {}", ruledefinition.getOutputsubjectname(), outputschema.toString(true), e);
							throw new KafkaException("Exception when writing the schema for subject name <" + ruledefinition.getOutputsubjectname() + ">", e);
						}
						schemadata = new SimpleEntry<>(outputschemaid, outputschema);
						inoutschema.put(schemaid, schemadata);
						schemaid = outputschemaid;
					} else {
						schemaid = schemadata.getKey();
					}
					try {
						inputrecord = JexlAvroDeserializer.deserialize(data, writerschema, schemadata.getValue());
					} catch (IOException e) {
						log.error("Exception when deserializing the byte[] into an Avro record\ndata: {}\ninputschema: {}\noutputschema: {}", data, writerschema.toString(true), writerschema.toString(true), e);
						throw new KafkaException("Exception when deserializing the byte[] into an Avro record", e);
					}
				}
			} else {
				/*
				 * The input has an AUDIT structure already, deserialize it into a JexRecord, apply the rules and serialize it into the same schema
				 */
				try {
					inputrecord = JexlAvroDeserializer.deserialize(data, writerschema);
				} catch (IOException e) {
					log.error("Exception when deserializing the byte[] into an Avro record\ndata: {}\ninputschema: {}\noutputschema: {}", data, writerschema.toString(true), writerschema.toString(true), e);
					throw new KafkaException("Exception when deserializing the byte[] into an Avro record", e);
				}
			}

			try {
				ruledefinition.apply(inputrecord, false);
			} catch (IOException e) {
				log.error("Exception when applying the rules to the record\\nrulefile: {}\nrecord: {}", ruledefinition.getName(), inputrecord, e);
				throw new KafkaException("Exception when applying the rules to the record", e);
			}

			byte[] out;
			try {
				out = AvroSerializer.serialize(schemaid, inputrecord);
				rowsprocessed.incrementAndGet();
				lastprocessedtimestamp = System.currentTimeMillis();
				processingtime.addAndGet(lastprocessedtimestamp - startts);
				return Bytes.wrap(out);
			} catch (IOException e) {
				log.error("Exception when serializing the Avro record into a byte[]", e);
				throw new KafkaException("Exception when serializing the Avro record into a byte[]", e);
			}
		}
	}

	public long getRowsprocessed() {
		return rowsprocessed.get();
	}

	public long getLastprocessedtimestamp() {
		return lastprocessedtimestamp;
	}

	public Map<String, RuleFileDefinition> getRulefiledefinitions() {
		return ruledefinitions;
	}

	public String getInputtopicname() {
		return inputtopicname;
	}

	public String getOutputtopicname() {
		return outputtopicname;
	}

	public Exception getError() {
		return error;
	}

	public int getInstances() {
		return instances;
	}

	public List<String> getRulefiles() {
		return rulefiles;
	}

	public Long getAvgProcessingtime() {
		if (rowsprocessed.get() != 0) {
			return processingtime.get() / rowsprocessed.get();
		} else {
			return null;
		}
	}

}

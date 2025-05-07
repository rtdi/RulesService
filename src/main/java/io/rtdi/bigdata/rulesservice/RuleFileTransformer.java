package io.rtdi.bigdata.rulesservice;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.Schema;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.utils.Bytes;

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

/**
 * The RuleFileTransformer is a pipeline with
 * Thread(consumer --> Executor(apply rules) --> Future --> queue) --> queue reader --> producer
 */
public class RuleFileTransformer extends Thread {

	private static final int MAX_QUEUE_SIZE = 1000;

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
	private ArrayBlockingQueue<Future<Transformer>> queue;
	private Long starttime = null;
	private int numbercores;


	public RuleFileTransformer(List<String> rulefiles, RulesService service, String inputtopicname, String outputtopicname, Integer instances) throws IOException {
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
			starttime = System.currentTimeMillis();
			Thread reader = null;
			try {
				Properties consumerProperties = new Properties();
				consumerProperties.putAll(service.getKafkaProperties());
				consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "rulesservice_" + inputtopicname);
				consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
				consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
				consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
				consumerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, BytesSerializer.class.getName());
				consumerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, BytesSerializer.class.getName());


				try (KafkaProducer<Bytes, Bytes> producer = new KafkaProducer<>(consumerProperties);
						) {
					queue = new ArrayBlockingQueue<>(MAX_QUEUE_SIZE);
					try {
						KafkaReader kafkareader = new KafkaReader(queue, consumerProperties);
						reader = new Thread(kafkareader);
						reader.setName("Kafka Reader for " + inputtopicname);
						reader.start();
						int rowssent = 0;
						int producerpartitioncount = producer.partitionsFor(outputtopicname).size();
						long nextcommit = System.currentTimeMillis() + 60000L;
						Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
						while (reader.isAlive()) {
							while (!queue.isEmpty()) {
								LoggingUtil.LOGGER.debug("Taking a row from the transformed queue");
								Future<Transformer> future = queue.take();
								Transformer transformer = future.get();
								if (transformer.out != null) {
									LoggingUtil.LOGGER.debug("Got record with offset {} of topic_partition {}-{}", transformer.offset, transformer.topic, transformer.partition);
									/*
									 * Input partition should be preserved in case both have the same number of partitions
									 */
									producer.send(new ProducerRecord<Bytes, Bytes>(outputtopicname, producerpartitioncount == kafkareader.partitioncount ? transformer.partition : null, transformer.key, transformer.out));
									offsets.put(new TopicPartition(transformer.topic, transformer.partition), new OffsetAndMetadata(transformer.offset+1L));
									rowsprocessed.incrementAndGet();
									lastprocessedtimestamp = System.currentTimeMillis();
									processingtime.addAndGet(transformer.endts - transformer.startts);
									if (rowssent > 10000 || System.currentTimeMillis() > nextcommit) {
										LoggingUtil.LOGGER.debug("Committing consumer records");
										producer.flush();
										kafkareader.commit(offsets);
										rowssent = 0;
										nextcommit = System.currentTimeMillis() + 60000L;
										offsets = new HashMap<>();
									}
								}
							}
							if (System.currentTimeMillis() > nextcommit && offsets.size() > 0) {
								LoggingUtil.LOGGER.debug("Committing consumer records");
								producer.flush();
								kafkareader.commit(offsets);
								rowssent = 0;
								nextcommit = System.currentTimeMillis() + 60000L;
								offsets = new HashMap<>();
							} else if (offsets.size() == 0){
								Thread.sleep(1000); // if there is no data, wait a bit
							}
						}
						if (kafkareader.e != null) {
							this.error = kafkareader.e;
						}
					} finally {
						if (reader != null) {
							reader.interrupt();
							try {
								reader.join(5000);
							} catch (InterruptedException e) {
								LoggingUtil.LOGGER.warn("Exception when waiting for the reader thread to finish", e);
							}
						}
					}
				}
			} catch (InterruptedException e) {
				LoggingUtil.LOGGER.info("Service got stopped");
			} catch (Exception e) {
				LoggingUtil.LOGGER.error("Exception in Transformer thread", e);
				this.error = e;
			}
			starttime = null;
		}
	}

	/**
	 * The KafkaReader thread keeps putting records into the processing queue and, as the owner of the consumer, commits
	 * the offsets the producer has safely sent.
	 * In case there is a rebalance, the offsets might be about a partition the consumer no longer is subscribed to. Such an error
	 * is captured and the next commit of another offset-map will take care of this situation.
	 */
	private class KafkaReader implements Runnable {
		private ArrayBlockingQueue<Future<Transformer>> queue;
		private Exception e;
		private Properties consumerProperties;
		private boolean commitnow = false;
		private Map<TopicPartition, OffsetAndMetadata> offsets;
		private int partitioncount = 0;
		private int failedcommits;

		public KafkaReader(ArrayBlockingQueue<Future<Transformer>> queue, Properties consumerProperties) {
			this.queue = queue;
			this.consumerProperties = consumerProperties;
		}

		@Override
		public void run() {
			numbercores = Runtime.getRuntime().availableProcessors();
			try (KafkaConsumer<Bytes, Bytes> consumer = new KafkaConsumer<>(consumerProperties);
					ExecutorService executor = Executors.newWorkStealingPool(numbercores);) {
				ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {
					@Override
					public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
						if (offsets.size() != 0) {
							LoggingUtil.LOGGER.debug("Partitions revoked {}, hence committing the offsets", partitions);
							commit(consumer);
						}
					}

					@Override
					public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
					}
				};
				consumer.subscribe(List.of(inputtopicname), listener );
				this.partitioncount = consumer.partitionsFor(inputtopicname).size();
				failedcommits = 0;
				while (true) {
					ConsumerRecords<Bytes, Bytes> records = consumer.poll(Duration.ofSeconds(1));
					for (ConsumerRecord<Bytes, Bytes> record : records) {
						queue.add(executor.submit(new Transformer(record.value(), record.key(), record.offset(), record.topic(), record.partition())));
						LoggingUtil.LOGGER.debug("Queued record with offset {} of topic_partition {}-{}", record.offset(), record.topic(), record.partition());
					}
					if (commitnow) {
						commit(consumer);
					}
				}
			} catch (Exception ex) {
				LoggingUtil.LOGGER.error("Exception when consuming the record", ex);
				e = ex;
			}
		}

		private void commit(KafkaConsumer<Bytes, Bytes> consumer) {
			commitnow = false; // These offsets should be committed once, because it might contain partitions the consumer is no longer in charge
			if (offsets.size() != 0) {
				try {
					LoggingUtil.LOGGER.debug("Commit was requested for offsets {}", offsets);
					consumer.commitSync(offsets);
					LoggingUtil.LOGGER.debug("Commit was completed for offsets {}", offsets);
					failedcommits = 0;
				} catch (RebalanceInProgressException | CommitFailedException e) {
					failedcommits++;
					if (failedcommits > 5) {
						LoggingUtil.LOGGER.error("Exception when committing the offsets due to rebalance related operations for the fifth time in a row, offsets = {}", offsets, e);
						throw e;
					} else {
						LoggingUtil.LOGGER.warn("Exception when committing the offsets due to rebalance related operations - ignored, offsets = {}", offsets, e);
					}
				}
			}
		}

		public void commit(Map<TopicPartition, OffsetAndMetadata> offsets) {
			commitnow = true;
			this.offsets = offsets;
		}
	}

	private class Transformer implements Callable<Transformer> {
		public Bytes in;
		public Bytes out;
		public Bytes key;
		public long offset;
		public long startts;
		public long endts;
		private String topic;
		private int partition;

		public Transformer(Bytes in, Bytes key, long offset, String topic, int partition) {
			this.in = in;
			this.key = key;
			this.offset = offset;
			this.topic = topic;
			this.partition = partition;
		}

		@Override
		public Transformer call() throws Exception {
			startts = System.currentTimeMillis();
			byte[] data = in.get();

			int schemaid;
			try {
				schemaid = AvroDeserializer.getSchemaId(data);
			} catch (IOException e) {
				LoggingUtil.LOGGER.error("This input is not a valid Avro record with a magic byte", e);
				throw new KafkaException("This input is not a valid Avro record with a magic byte", e);
			}

			ParsedSchema writerparsedschema;
			try {
				writerparsedschema = service.getSchemaclient().getSchemaById(schemaid);
			} catch (IOException | RestClientException e) {
				LoggingUtil.LOGGER.error("Exception when reading the schema <{}> from the schema registry by id", schemaid, e);
				throw new KafkaException("Exception when reading the schema <" + schemaid + "> from the schema registry by id", e);
			}
			Schema writerschema = (Schema) writerparsedschema.rawSchema();

			RuleFileDefinition ruledefinition = ruledefinitions.get(writerschema.getFullName());
			if (ruledefinition == null) {
				/*
				 * No transformations for that schema, hence a pass-through
				 */
				out = in;
				return this;
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
								LoggingUtil.LOGGER.error("Exception when registering the schema for subject name <{}>\nschema: {}", ruledefinition.getOutputsubjectname(), outputschema.toString(true), e);
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
							LoggingUtil.LOGGER.error("Exception when deserializing the byte[] into an Avro record\ndata: {}\ninputschema: {}\noutputschema: {}", data, writerschema.toString(true), writerschema.toString(true), e);
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
						LoggingUtil.LOGGER.error("Exception when deserializing the byte[] into an Avro record\ndata: {}\ninputschema: {}\noutputschema: {}", data, writerschema.toString(true), writerschema.toString(true), e);
						throw new KafkaException("Exception when deserializing the byte[] into an Avro record", e);
					}
				}

				try {
					ruledefinition.apply(inputrecord, false);
				} catch (IOException e) {
					LoggingUtil.LOGGER.error("Exception when applying the rules to the record\\nrulefile: {}\nrecord: {}", ruledefinition.getName(), inputrecord, e);
					throw new KafkaException("Exception when applying the rules to the record", e);
				}

				try {
					out = Bytes.wrap(AvroSerializer.serialize(schemaid, inputrecord));
					endts = System.currentTimeMillis();
					return this;
				} catch (IOException e) {
					LoggingUtil.LOGGER.error("Exception when serializing the Avro record into a byte[]", e);
					throw new KafkaException("Exception when serializing the Avro record into a byte[]", e);
				}
			}
		}
	}

	public long getRowsprocessed() {
		return rowsprocessed.get();
	}

	public long getLastprocessedtimestamp() {
		return lastprocessedtimestamp;
	}

	public int getQueuedRecordCount() {
		if (queue == null) {
			return 0;
		} else {
			return queue.size();
		}
	}

	public int getQueuecapacity() {
		return MAX_QUEUE_SIZE;
	}

	public int getNumbercores() {
		return numbercores;
	}

	public Float getRowspersecond() {
		if (starttime == null) {
			return null;
		} else {
			long elapsed = System.currentTimeMillis() - starttime;
			if (elapsed == 0) {
				return null;
			}
			float rowspersecond = rowsprocessed.get() / (elapsed / 1000.0f);
			return rowspersecond;
		}
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

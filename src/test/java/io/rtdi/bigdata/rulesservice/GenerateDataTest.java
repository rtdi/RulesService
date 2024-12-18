package io.rtdi.bigdata.rulesservice;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

class GenerateDataTest {

	private static Map<String, Object> propertiesmap;
	private static CachedSchemaRegistryClient schemaclient;
	private static Admin admin;
	@BeforeAll
	static void setUpBeforeClass() throws Exception {
		Path settingsdir = Path.of("/apps/rulesservice/settings");
		Path.of("/apps/rulesservice/definitions");
		File propertiesfile = new File(settingsdir.toFile(), "kafka.properties");
		if (!propertiesfile.isFile()) {
			throw new IOException("The mandatory kafka.properties file does not exist at <" + propertiesfile.toString() + ">");
		}
		Properties kafkaproperties = new Properties();
		try (InputStream is = new FileInputStream(propertiesfile)) {
			kafkaproperties.load(is);
		}
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
		propertiesmap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
		propertiesmap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

		schemaclient = new CachedSchemaRegistryClient(schemaRegistryUrls, 100, propertiesmap);
		schemaclient.getMode(); // invoke schema registry to validate connection
		admin = Admin.create(kafkaproperties);
		admin.describeCluster();
	}

	@AfterAll
	static void tearDownAfterClass() throws Exception {
		if (schemaclient != null) {
			schemaclient.close();
		}
		if (admin != null) {
			admin.close();
		}
	}

	@Test
	void test() {
		try (Producer<Integer, GenericRecord> producer = new KafkaProducer<Integer, GenericRecord>(propertiesmap);) {
			SchemaMetadata schemametadata = schemaclient.getLatestSchemaMetadata("adresses-value");
			String schemastring = schemametadata.getSchema();
			Schema schema = new Schema.Parser().parse(schemastring);
			int id = 1;
			GenericRecord record = new GenericData.Record(schema);
			/*
				{
					"id": 1,
					"firstname": "Werner",
					"lastname": "rtdi",
					"addresses": [
						{
							"addresstype": "HOME",
							"addressline1": "whatsoever 34",
							"city": "Montreal",
							"state": null,
							"zipcode": "H1A 0A1",
							"country": "CA"
						}
					]
				}
			 */
			record.put("id", id);
			record.put("firstname", "Werner");
			record.put("lastname", "rtdi");
			Schema addressschema = record.getSchema().getField("addresses").schema().getTypes().get(1).getElementType();
			List<GenericRecord> addresses = new ArrayList<>();
			GenericRecord homeaddress = new GenericData.Record(addressschema);
			homeaddress.put("addresstype", "HOME");
			homeaddress.put("addressline1", "whatsoever 34");
			homeaddress.put("city", "Montreal");
			homeaddress.put("state", null);
			homeaddress.put("zipcode", "H1A 0A1");
			homeaddress.put("country", "CA");
			addresses.add(homeaddress);
			record.put("addresses", addresses);
			producer.send(new ProducerRecord<Integer, GenericRecord>("adresses", id, record));
		} catch (Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}

}

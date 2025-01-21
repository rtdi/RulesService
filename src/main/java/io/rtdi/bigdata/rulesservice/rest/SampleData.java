package io.rtdi.bigdata.rulesservice.rest;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Bytes;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.rtdi.bigdata.kafka.avro.AvroDeserializer;
import io.rtdi.bigdata.rulesservice.config.SampleFileName;
import io.rtdi.bigdata.rulesservice.config.SubjectName;
import io.rtdi.bigdata.rulesservice.jexl.AvroRuleUtils;
import io.rtdi.bigdata.rulesservice.jexl.JexlAvroDeserializer;
import io.rtdi.bigdata.rulesservice.jexl.JexlRecord;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SampleData {

	private long offset;
	private String topic;
	private int partition;
	private String filename;
	private String payload;
	private String subjectname;
	private List<SubjectName> subjects;
	private String schema;

	public SampleData() {
		super();
	}

	public SampleData(ConsumerRecord<Bytes, Bytes> recordset, CachedSchemaRegistryClient schemaclient) throws IOException, RestClientException {
		Bytes value = recordset.value();
		byte[] data = value.get();
		int schemaid = AvroDeserializer.getSchemaId(data);
		ParsedSchema writerparsedschema = schemaclient.getSchemaById(schemaid);
		Collection<String> subjects = schemaclient.getAllSubjectsById(schemaid);
		Schema writerschema = (Schema) writerparsedschema.rawSchema();
		JexlRecord foundrecord = JexlAvroDeserializer.deserialize(data, writerschema);
		this.offset = recordset.offset();
		this.topic = recordset.topic();
		this.partition = recordset.partition();
		this.filename = null;
		this.payload = foundrecord.toString(); // convert to a simple json without data types
		// this.payload = AvroRuleUtils.convertToJson(foundrecord, writerschema);
		this.setSchema(writerschema.toString());
		if (subjects != null && subjects.size() > 0) {
			Iterator<String> iter = subjects.iterator();
			this.subjects = new ArrayList<>();
			while (iter.hasNext()) {
				String subject = iter.next();
				this.subjectname = subject;
				this.subjects.add(new SubjectName(subject));
			}
		}
	}

	public String save(Path rootdir) throws IOException {
		if (filename == null || filename.length() == 0) {
			filename = "partition_" + partition + "_offset_" + offset + ".json";
		} else if (filename.contains("..")) {
			throw new IOException("filename can only contain subdirectories, not <..>");
		}

		ObjectMapper om = new ObjectMapper();
		Path dirpath = rootdir.resolve(subjectname).resolve("sampledata");
		dirpath.toFile().mkdirs();
		Path filepath = dirpath.resolve(filename);
		om.writer()
		.withDefaultPrettyPrinter()
		.writeValue(filepath.toFile(), this);
		return filename;
	}

	public static List<SampleFileName> getFiles(Path rootdir, String subjectname) {
		Path dirpath = rootdir.resolve(subjectname).resolve("sampledata");
		List<SampleFileName> ret = new ArrayList<>();
		collectSampleFiles(dirpath, ret, null);
		return ret;
	}

	private static void collectSampleFiles(Path dirpath, List<SampleFileName> ret, String relativepath) {
		File[] files = dirpath.toFile().listFiles();
		if (files != null) {
			for (File f : files) {
				String filename = (relativepath == null ? "" : relativepath + "/") + f.getName();
				if (f.isDirectory()) {
					collectSampleFiles(f.toPath(), ret, filename);
				} else {
					ret.add(new SampleFileName(filename));
				}
			}
		}
	}

	public static SampleData load(Path rootdir, String subjectname, Path filename) throws IOException {
		Path filepath = rootdir.resolve(subjectname).resolve("sampledata").resolve(filename);
		ObjectMapper om = new ObjectMapper();
		return om.readValue(filepath.toFile(), SampleData.class);
	}

	public String getSubjectname() {
		return subjectname;
	}

	public void setSubjectname(String subjectname) {
		this.subjectname = subjectname;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public int getPartition() {
		return partition;
	}

	public void setPartition(int partition) {
		this.partition = partition;
	}

	public String getFilename() {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}

	public String getPayload() {
		return payload;
	}

	public void setPayload(String payload) {
		this.payload = payload;
	}

	public List<SubjectName> getSubjects() {
		return subjects;
	}

	public void setSubjects(List<SubjectName> subjects) {
		this.subjects = subjects;
	}

	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	public JexlRecord createRecord(Schema targetschema) throws IOException {
		Parser parser = new Parser();
		Schema sourceschema;
		if (schema != null) {
			sourceschema = parser.parse(schema);
		} else {
			sourceschema = targetschema;
		}
		JexlRecord jexlrecord = AvroRuleUtils.convertFromJson(payload, sourceschema);
		return jexlrecord;
	}

}

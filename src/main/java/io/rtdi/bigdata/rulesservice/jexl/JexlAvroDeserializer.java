package io.rtdi.bigdata.rulesservice.jexl;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.ResolvingDecoder;

import io.rtdi.bigdata.kafka.avro.AvroDeserializer;
import io.rtdi.bigdata.kafka.avro.AvroUtils;

/**
 * This is class does deserialize Avro Jexl records. It uses the same format as Kafka itself, hence
 * even if data is serialized by others, e.g. Kafka Connect, it can be consumed.
 *
 */
public class JexlAvroDeserializer extends AvroDeserializer {

	private static final DecoderFactory decoderFactory = DecoderFactory.get();

	/**
	 * Converts a byte[] into an Avro JexlRecord using the supplied schema.
	 * The schema must be read from the schema registry using the message's schema id, see {@link #getSchemaId(byte[])}
	 *
	 * @param data with the binary Avro representation
	 * @param schema used for the deserialization
	 * @return JexlRecord in Jexl abstraction
	 * @throws IOException in case this is not a valid Avro Kafka message
	 */
	public static JexlRecord deserialize(byte[] data, Schema schema) throws IOException {
		if (data != null) {
			try (ByteArrayInputStream in = new ByteArrayInputStream(data); ) {
				int b = in.read();
				if (b != AvroUtils.MAGIC_BYTE) {
					throw new AvroRuntimeException("Not a valid Kafka Avro message frame");
				} else {
					in.skip(Integer.BYTES);
					BinaryDecoder decoder = decoderFactory.directBinaryDecoder(in, null);
					DatumReader<JexlRecord> reader = new JexlGenericDatumReader<>(schema);
					return reader.read(null, decoder);
				}
			}
		} else {
			return null;
		}
	}

	/**
	 * Converts a byte[] into an Avro JexlRecord using the supplied schema.
	 * The schema must be read from the schema registry using the message's schema id, see {@link #getSchemaId(byte[])}
	 *
	 * @param data with the binary Avro representation
	 * @param schemawriter used for the deserialization
	 * @param schemareader the evolved schema
	 * @return JexlRecord in Jexl abstraction
	 * @throws IOException in case this is not a valid Avro Kafka message
	 */
	public static JexlRecord deserialize(byte[] data, Schema schemawriter, Schema schemareader) throws IOException {
		if (data != null) {
			try (ByteArrayInputStream in = new ByteArrayInputStream(data); ) {
				int b = in.read();
				if (b != AvroUtils.MAGIC_BYTE) {
					throw new AvroRuntimeException("Not a valid Kafka Avro message frame");
				} else {
					in.skip(Integer.BYTES);
					ResolvingDecoder decoder = decoderFactory.resolvingDecoder(schemawriter, schemareader, decoderFactory.directBinaryDecoder(in, null));
					DatumReader<JexlRecord> reader = new JexlGenericDatumReader<>(schemareader);
					return reader.read(null, decoder);
				}
			}
		} else {
			return null;
		}
	}

}

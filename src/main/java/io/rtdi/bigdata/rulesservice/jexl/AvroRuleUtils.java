package io.rtdi.bigdata.rulesservice.jexl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilderException;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.commons.text.StringEscapeUtils;

import io.rtdi.bigdata.kafka.avro.RowType;
import io.rtdi.bigdata.kafka.avro.RuleResult;
import io.rtdi.bigdata.kafka.avro.SchemaConstants;
import io.rtdi.bigdata.kafka.avro.recordbuilders.ValueSchema;

public class AvroRuleUtils {

	public static String convertRecordToJson(GenericRecord record) throws IOException {
		return record.toString();
	}

	public static String encodeJson(String text) {
		/*
		 * Backspace is replaced with \b
		 * Form feed is replaced with \f
		 * Newline is replaced with \n
		 * Carriage return is replaced with \r
		 * Tab is replaced with \t
		 * Double quote is replaced with \"
		 * Backslash is replaced with \\
		 */
		return StringEscapeUtils.escapeJson(text);
	}

	public static void mergeResults(JexlRecord valuerecord) throws IOException {
		try {
			List<JexlRecord> auditdetails = valuerecord.getRuleresults();
			JexlRecord auditrecord = (JexlRecord) valuerecord.get(ValueSchema.AUDIT);
			RuleResult globalresult;
			if (auditrecord == null) {
				auditrecord = new JexlRecord(ValueSchema.audit.getSchema(), valuerecord);
				globalresult = RuleResult.PASS;
				valuerecord.put(ValueSchema.AUDIT, auditrecord);
			} else {
				globalresult = getRuleResult(auditrecord);
			}
			@SuppressWarnings("unchecked")
			JexlArray<JexlRecord> details = (JexlArray<JexlRecord>) auditrecord.get(ValueSchema.AUDITDETAILS);
			if (details == null) {
				details = new JexlArray<JexlRecord>(100, ValueSchema.auditdetails_array_schema, valuerecord);
				auditrecord.put(ValueSchema.AUDITDETAILS, details);
			}
			for ( JexlRecord d : auditdetails) {
				details.add(d);
				RuleResult ruleresult = getRuleResult(d);
				/*
				 * The individual rule result pulls down the global result PASS -> WARN -> FAIL
				 * but never _corrects_ a result.
				 * Examples:
				 * GLOBAL + INDIVIDUAL = NEW GLOBAL
				 * PASS + PASS = PASS
				 * PASS + WARN = WARN
				 * PASS + FAIL = FAIL
				 * WARN + PASS = WARN
				 * WARN + WARN = WARN
				 * WARN + FAIL = FAIL
				 * FAIL + xxxx = FAIL
				 */
				if (ruleresult == RuleResult.FAIL) {
					globalresult = RuleResult.FAIL;
				} else if (ruleresult == RuleResult.WARN && globalresult == RuleResult.PASS) {
					globalresult = RuleResult.WARN;
				}
			}
			auditrecord.put(ValueSchema.TRANSFORMRESULT, globalresult.name());
		} catch (SchemaBuilderException e) {
			throw new IOException("Cannot merge audit data", e);
		}
	}

	public static JexlRecord createAuditDetails() {
		return new JexlRecord(ValueSchema.auditdetails_array_schema.getElementType(), null);
	}

	public static RowType getChangeType(JexlRecord valuerecord) {
		if (valuerecord.hasField(SchemaConstants.SCHEMA_COLUMN_CHANGE_TYPE)) {
			Object changetype = valuerecord.get(SchemaConstants.SCHEMA_COLUMN_CHANGE_TYPE);
			if (changetype != null) {
				return RowType.getByIdentifier(changetype.toString());
			}
		}
		return null;
	}

	public static RuleResult getRuleResult(JexlRecord record) {
		Object g = record.get(ValueSchema.TRANSFORMRESULT);
		RuleResult ruleresult;
		if (g == null) {
			ruleresult = RuleResult.PASS;
		} else {
			String s = g.toString();
			ruleresult = RuleResult.valueOf(s);
		}
		return ruleresult;
	}

	public static JexlRecord convertFromJson(String json, Schema schema) throws IOException {
		DatumReader<JexlRecord> reader = new JexlGenericDatumReader<>(schema);
		JsonDecoder decoder = DecoderFactory.get().jsonDecoder(schema, json);
		return reader.read(null, decoder);
	}

	public static String convertToJson(JexlRecord record, Schema schema) throws IOException {
		DatumWriter<JexlRecord> writer = new GenericDatumWriter<>(schema);
		try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
			JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(schema, stream);
			writer.write(record, jsonEncoder);
			jsonEncoder.flush();
			return stream.toString();
		}
	}

}

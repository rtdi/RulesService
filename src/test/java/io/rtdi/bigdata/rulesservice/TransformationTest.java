package io.rtdi.bigdata.rulesservice;

import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Path;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlRecord;
import io.rtdi.bigdata.kafka.avro.RuleResult;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroNVarchar;
import io.rtdi.bigdata.kafka.avro.recordbuilders.SchemaBuilder;
import io.rtdi.bigdata.kafka.avro.recordbuilders.ValueSchema;
import io.rtdi.bigdata.rulesservice.definition.RuleFileDefinition;
import io.rtdi.bigdata.rulesservice.definition.RuleStep;
import io.rtdi.bigdata.rulesservice.rules.PrimitiveRule;

class TransformationTest {

	@BeforeAll
	static void setUpBeforeClass() throws Exception {
	}

	@AfterAll
	static void tearDownAfterClass() throws Exception {
	}

	@Test
	void test() {
		try {
			SchemaBuilder schemabuilder = new ValueSchema("ADDRESS", "Address table");
			schemabuilder.add("POSTALCODE", AvroNVarchar.getSchema(5), null, false, null);
			schemabuilder.add("COUNTRY", AvroNVarchar.getSchema(40), null, false, null);
			schemabuilder.build();

			RuleFileDefinition rg = new RuleFileDefinition(Path.of("Address validation.json"), "CUSTOMER");
			RuleStep step1 = new RuleStep("01_CLEANSE", schemabuilder.getSchema());
			rg.addRuleStep(step1);
			PrimitiveRule rule1 = new PrimitiveRule("POSTALCODE", "Validate Format", "POSTALCODE != null && POSTALCODE.length() == 5", RuleResult.WARN, null, schemabuilder.getSchema().getField("POSTALCODE").schema());
			PrimitiveRule rule2 = new PrimitiveRule("COUNTRY", "Validate country not empty", "COUNTRY != null", RuleResult.FAIL, null, schemabuilder.getSchema().getField("COUNTRY").schema());
			step1.addRule(rule1);
			step1.addRule(rule2);

			JexlRecord record1 = new JexlRecord(schemabuilder.getSchema(), null);
			record1.set("POSTALCODE", "81959");
			record1.set("COUNTRY", "GERMANY");
			rg.setSchema(schemabuilder.getSchema().toString());
			rg.apply(record1, false);
			System.out.println(record1);

			rg.save(Path.of("src/test/resources/tmp/rules"), null);

		} catch (Exception e) {
			e.printStackTrace();
			fail("Not yet implemented");
		}
	}

}

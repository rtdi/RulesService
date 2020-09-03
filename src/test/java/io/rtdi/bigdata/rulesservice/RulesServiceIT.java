package io.rtdi.bigdata.rulesservice;

import static org.junit.Assert.fail;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;
import io.rtdi.bigdata.connector.connectorframework.controller.ServiceController;
import io.rtdi.bigdata.connector.pipeline.foundation.PipelineAbstract;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaRegistryName;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.RuleResult;
import io.rtdi.bigdata.connectors.pipeline.kafkadirect.KafkaAPIdirect;
import io.rtdi.bigdata.connectors.pipeline.kafkadirect.KafkaConnectionProperties;
import io.rtdi.bigdata.rulesservice.rules.ArrayRule;
import io.rtdi.bigdata.rulesservice.rules.PrimitiveRule;
import io.rtdi.bigdata.rulesservice.rules.TestSetFirstPass;

public class RulesServiceIT {
	private PipelineAbstract<?,?,?,?> api;

	@Before
	public void setUp() throws Exception {
		File dir = new File("./src/test/resources/tmp");
		KafkaConnectionProperties kafkaprops = new KafkaConnectionProperties();
		kafkaprops.read(dir);
		api = new KafkaAPIdirect(kafkaprops);
		api.open();
	}

	@After
	public void tearDown() throws Exception {
		api.close();
	}
	
	@Test
	public void testRulesService() {
		try {
			ConnectorController connectorcontroller = new ConnectorController(new RulesServiceFactory(), "./src/test/resources/tmp", null);
			connectorcontroller.setAPI(api);
			RulesServiceProperties properties = new RulesServiceProperties("RulesTest1");
			properties.setSourceTopic("SALES");
			properties.setTargetTopic("SALES_CLEANSED");
			ServiceController servicecontroller = new ServiceController(properties , connectorcontroller);
			connectorcontroller.addChild(properties.getName(), servicecontroller);
			
			RuleStep rulestep = new RuleStep("Step1");
			properties.addMicroService(rulestep);
			SchemaRuleSet ruleset = rulestep.getSchemaRule(SchemaRegistryName.create("schema1"));
			
			ruleset.addRule(new TestSetFirstPass("SoldTo", "Test SoldTo column", new PrimitiveRule("Test1", "SoldTo is not null", "SoldTo != null", RuleResult.FAIL, null)));
			ruleset.addRule(new TestSetFirstPass("BillTo", "Test BillTo column", new PrimitiveRule("Test1", "BillTo is not null", "BillTo != null", RuleResult.FAIL, null)));
			ArrayRule c = ruleset.addNested("SalesItems");
			c.addRule(new TestSetFirstPass("MaterialNumber", "Check material not null", new PrimitiveRule("Test1", "MaterialNumber is not null", "MaterialNumber != null", RuleResult.FAIL, null)));
			c.addRule(new TestSetFirstPass("Quantity", "Check qty not null", new PrimitiveRule("Test1", "QTY is not null", "Quantity != null", RuleResult.FAIL, null)));
			
			File dir = new File("./src/test/resources/tmp/services");
			if (!dir.exists()) {
				dir.mkdir();
			}
			properties.write(dir, null);
			
			RulesServiceProperties properties2 = new RulesServiceProperties("RulesTest1");
			properties2.read(dir);
			System.out.println(properties2.toString());
			// connectorcontroller.startController(false);
			
			// Thread.sleep(240000);
			
			// connectorcontroller.stopController(ControllerExitType.ENDBATCH);
			
			// System.out.println("Got " + data.size() + " records");
			// assertTrue(data.size() <= 10);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

}

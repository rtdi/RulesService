package io.rtdi.bigdata.rulesservice.config;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import io.rtdi.bigdata.rulesservice.RuleFileTransformer;
import io.rtdi.bigdata.rulesservice.RulesService;

public class ServiceStatus {
	private int activetopics = 0;
	private List<ServiceStatusTopic> topicstatus;
	private ServiceSettings config;

	public ServiceStatus(RulesService service) {
		Map<String, TopicRule> rulefiles = null;
		try {
			rulefiles = service.getTopicRuleFiles();
		} catch (IOException e) {
			rulefiles = new HashMap<>();
		}

		Map<String, RuleFileTransformer> services = service.getServices();
		config = service.getConfig(false);
		if (services != null) {
			topicstatus = new ArrayList<>();
			for (Entry<String, RuleFileTransformer> entity : services.entrySet()) {
				ServiceStatusTopic s = new ServiceStatusTopic(entity.getValue());
				if (s.getStatus() == Boolean.TRUE) {
					activetopics++;
				}
				topicstatus.add(s);
				rulefiles.remove(entity.getKey());
			}
		}
		for (String topic : rulefiles.keySet()) {
			ServiceStatusTopic s = new ServiceStatusTopic(topic);
			topicstatus.add(s);
		}
	}

	public List<ServiceStatusTopic> getTopicstatus() {
		return topicstatus;
	}

	public int getActivetopics() {
		return activetopics;
	}

	public ServiceSettings getConfig() {
		return config;
	}

}

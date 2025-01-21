package io.rtdi.bigdata.rulesservice.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import io.rtdi.bigdata.rulesservice.RuleFileKStream;
import io.rtdi.bigdata.rulesservice.RulesService;

public class ServiceStatus {
	private int activetopics = 0;
	private List<ServiceStatusTopic> topicstatus;
	private ServiceSettings config;

	public ServiceStatus(RulesService service) {
		Map<String, RuleFileKStream> services = service.getServices();
		config = service.getConfig(false);
		if (services != null) {
			topicstatus = new ArrayList<>();
			for (Entry<String, RuleFileKStream> entity : services.entrySet()) {
				ServiceStatusTopic s = new ServiceStatusTopic(entity.getValue());
				if (s.getStatus() == Boolean.TRUE) {
					activetopics++;
				}
				topicstatus.add(s);
			}
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

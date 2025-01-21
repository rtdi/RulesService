package io.rtdi.bigdata.rulesservice.config;

import java.util.ArrayList;
import java.util.List;

import io.rtdi.bigdata.rulesservice.RuleFileKStream;

public class ServiceStatusTopic {
	private Boolean status;
	private List<String> activerules;
	private long rowsprocessed = 0L;
	private long lastprocessedtimestamp = 0L;
	private Long avgtime = null;
	private String topicname;
	private String name;
	private Integer rulecount = 0;

	public ServiceStatusTopic(RuleFileKStream stream) {
		topicname = stream.getInputtopicname();
		name = stream.getName();
		if (stream.isAlive()) {
			status = Boolean.TRUE;
			rowsprocessed = stream.getRowsprocessed();
			lastprocessedtimestamp = stream.getLastprocessedtimestamp();
			avgtime = stream.getAvgProcessingtime();
			activerules = new ArrayList<>();
			for (RuleFileDefinition r : stream.getRulefiledefinitions().values()) {
				activerules.add(r.getName());
			}
			rulecount = activerules.size();
		} else {
			status = Boolean.FALSE;
		}
	}

	public Boolean getStatus() {
		return status;
	}

	public List<String> getActiverules() {
		return activerules;
	}

	public long getRowsprocessed() {
		return rowsprocessed;
	}

	public long getLastprocessedtimestamp() {
		return lastprocessedtimestamp;
	}

	public Long getAvgtime() {
		return avgtime;
	}

	public String getTopicname() {
		return topicname;
	}

	public String getName() {
		return name;
	}

	public Integer getRulecount() {
		return rulecount;
	}
}

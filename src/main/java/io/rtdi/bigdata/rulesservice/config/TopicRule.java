package io.rtdi.bigdata.rulesservice.config;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.exc.StreamWriteException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.rtdi.bigdata.rulesservice.definition.RuleFileDefinition;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TopicRule implements Comparable<TopicRule> {
	private String inputtopicname;
	private String outputtopicname;
	private Boolean modified;
	private Boolean activate;
	private String info;
	/**
	 * A List of rulefiles to be used for this topic in the format of <subject>/<filename>
	 */
	private List<String> rulefiles;

	public TopicRule(String topicname) {
		this.inputtopicname = topicname;
	}

	public TopicRule() {
	}

	public TopicRule(TopicRule source) {
		this.inputtopicname = source.inputtopicname;
		this.outputtopicname = source.outputtopicname;
		this.rulefiles = source.rulefiles;
	}

	public String getInputtopicname() {
		return inputtopicname;
	}

	public void setInputtopicname(String topicname) {
		this.inputtopicname = topicname;
	}

	public List<String> getRulefiles() {
		return rulefiles;
	}

	public void setRulefiles(List<String> rulefiles) {
		this.rulefiles = rulefiles;
	}

	public String getOutputtopicname() {
		return outputtopicname;
	}

	public void setOutputtopicname(String outputtopicname) {
		this.outputtopicname = outputtopicname;
	}

	public Boolean getModified() {
		return modified;
	}

	public void setModified(Boolean modified) {
		this.modified = modified;
	}

	public void save(Path topicruledir) throws StreamWriteException, DatabindException, IOException {
		ObjectMapper om = new ObjectMapper();
		File file = topicruledir.resolve(inputtopicname + ".json").toFile();
		TopicRule clone = new TopicRule(this);
		om.writer()
		.withDefaultPrettyPrinter()
		.writeValue(file, clone);
		if (activate != null && activate) {
			for (String rulefile : rulefiles) {
				int pos = rulefile.indexOf('/');
				if (pos > 0) {
					String subject = rulefile.substring(0, pos);
					String filename = rulefile.substring(pos+1);
					RuleFileDefinition.copyToActivate(topicruledir.getParent(), subject, filename);
				} else {
					throw new IOException("The selected key does not follow the path convention <subject>/<filename>");
				}
			}
		}
	}

	@Override
	public int compareTo(TopicRule o) {
		if (o == null) {
			return -1;
		} else {
			return o.inputtopicname.compareTo(inputtopicname);
		}
	}

	public void delete(Path topicruledir) {
		File file = topicruledir.resolve(inputtopicname + ".json").toFile();
		if (file.exists()) {
			file.delete();
		}
	}

	public String getInfo() {
		return info;
	}

	public void setInfo(String info) {
		this.info = info;
	}

	public Boolean getActivate() {
		return activate;
	}

	public void setActivate(Boolean activate) {
		this.activate = activate;
	}

}

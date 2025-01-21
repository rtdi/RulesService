package io.rtdi.bigdata.rulesservice.config;

import java.nio.file.attribute.FileTime;

public class RuleFileName {
	private String name;
	private String filetimeinactive;
	private String filetimeactive;
	private String subjectname;
	private Boolean active;

	public RuleFileName(String name, String subjectname, FileTime inactivetime, FileTime activetime) {
		this.name = name;
		this.subjectname = subjectname;
		if (inactivetime != null) {
			this.filetimeinactive = inactivetime.toString();
		}
		if (activetime != null) {
			this.filetimeactive = activetime.toString();
			this.active = activetime.compareTo(inactivetime) >= 0;
		} else {
			this.active = Boolean.FALSE;
		}
	}

	public String getName() {
		return name;
	}

	public String getFiletimeinactive() {
		return filetimeinactive;
	}

	public String getFiletimeactive() {
		return filetimeactive;
	}

	public String getSubjectname() {
		return subjectname;
	}

	public Boolean getActive() {
		return active;
	}

}

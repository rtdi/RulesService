package io.rtdi.bigdata.rulesservice.config;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.rtdi.bigdata.kafka.avro.RowType;
import io.rtdi.bigdata.rulesservice.RulesService;
import io.rtdi.bigdata.rulesservice.jexl.AvroRuleUtils;
import io.rtdi.bigdata.rulesservice.jexl.JexlRecord;
import io.rtdi.bigdata.rulesservice.rules.RuleUtils;

public class RuleFileDefinition {
	private Path name;
	private String inputsubjectname;
	private String outputsubjectname;
	private String samplefile;
	private String schema;
	private Schema schemadef;

	private List<RuleStep> rulesteps;
	/**
	 * A Definition can be saved while still not complete - then the previous version is to be used
	 */
	private boolean active = false;

	/**
	 * @param name depicts a relative path to the json file
	 * @param inputsubjectname is the name of the subject this rule group assumes as input
	 */
	public RuleFileDefinition(Path name, String inputsubjectname) {
		this.name = name;
		this.inputsubjectname = inputsubjectname;
	}

	public RuleFileDefinition() {
	}

	public RuleFileDefinition(Schema schema) {
		schemadef = schema;
		this.schema = schema.toString();
	}

	public List<RuleStep> getRulesteps() {
		return rulesteps;
	}

	public void setRulesteps(List<RuleStep> rulesteps) {
		this.rulesteps = rulesteps;
	}

	public JexlRecord apply(JexlRecord valuerecord, boolean test) throws IOException {
		RowType changetype = AvroRuleUtils.getChangeType(valuerecord);
		// Rules are applied to new or changed records but not deleted rows
		if (rulesteps != null && rulesteps.size() > 0 && (changetype == null || (changetype != RowType.EXTERMINATE && changetype != RowType.DELETE && changetype != RowType.TRUNCATE))) {
			for (RuleStep step : rulesteps) {
				/*
				 * Go through one rule step, collect the rule results to be stored in the _audit record and also the changed values.
				 * The changed values are collected first and applied in a second step to avoid side effects when one rule modifies
				 * a value and another reads it.
				 */
				step.apply(valuerecord, valuerecord, test);
				valuerecord.mergeReplacementValues();
				if (test) {
					step.updateSampleOutput(valuerecord);
				}
			}
			if (!test) {
				/*
				 * Take all collected rule results and add them to the record.
				 */
				AvroRuleUtils.mergeResults(valuerecord);
			}
		}
		return valuerecord;
	}

	/**
	 * @param rootdir where all files are located
	 * @param schemanname the rule requires as input
	 * @param name depicts a relative path to the json file
	 * @param active if true, it goes into the active folder and returns that version, else the inactive folder with intermediate files
	 * @return the RuleFileDefinition or null, if it not exists as active/inactive as requested
	 * @throws IOException
	 */
	public static RuleFileDefinition load(Path rootdir, String subjectname, Path name, boolean active) throws IOException {
		/*
		 * The directory structure is <rootdir>/<subjectname>/<active>/<name>/..../<name>
		 */
		if (name.isAbsolute()) {
			throw new IOException("name path <" + name + "> cannot be an absolute path");
		}
		String activedir = getActivePath(active);
		Path filepath = rootdir.resolve(subjectname).resolve(activedir).resolve(name);
		if (filepath.toFile().isFile()) {
			ObjectMapper om = new ObjectMapper();
			RuleFileDefinition rg = om.readValue(filepath.toFile(), RuleFileDefinition.class);
			rg.name = name;
			rg.inputsubjectname = subjectname;
			rg.setActive(active);
			return rg;
		} else {
			return null;
		}
	}

	private static String getActivePath(boolean active) {
		return active ? "active" : "inactive";
	}

	public void save(Path rootdir, Path filename) throws IOException {
		if (filename == null) {
			throw new IOException("The filename cannot be null");
		} else if (filename.toString().contains("..")) {
			throw new IOException("The filename can contain subdirectories but not <..>");
		}
		if (name == null) {
			throw new IOException("The name field cannot be null");
		} else if (name.toString().contains("..")) {
			throw new IOException("The name field can contain subdirectories but not <..>");
		}
		ObjectMapper om = new ObjectMapper();
		String activedir = getActivePath(active);
		Path path = rootdir.resolve(inputsubjectname).resolve(activedir).resolve(name); // name can have directories too
		File dir = path.getParent().toFile();
		dir.mkdirs(); // create all directories in case this is a new rule group

		om.writer()
		.withDefaultPrettyPrinter()
		.writeValue(path.toFile(), this); // save the file with the path in the object
		if (filename != null && !filename.equals(this.name)) {
			// file was renamed, delete the old
			path = rootdir.resolve(inputsubjectname).resolve(activedir).resolve(filename);
			File file = path.toFile();
			if (file.exists()) {
				file.delete();
			}
		}
	}

	@JsonIgnore
	public boolean isActive() {
		return active;
	}

	public void setActive(boolean active) {
		this.active = active;
	}

	public String getName() {
		if (name == null) {
			return "new";
		} else if (File.separatorChar == '\\') {
			return name.toString().replace('\\', '/');
		} else {
			return name.toString();
		}
	}

	public void setName(String name) {
		if (name.startsWith("/")) {
			name = name.substring(1);
		}
		this.name = Path.of(name);
	}

	@JsonIgnore
	public void setPath(Path path) {
		this.name = path;
	}

	public String getInputsubjectname() {
		return inputsubjectname;
	}

	public void setInputsubjectname(String inputsubjectname) {
		this.inputsubjectname = inputsubjectname;
	}

	public void addRuleStep(RuleStep step) {
		if (rulesteps == null) {
			rulesteps = new ArrayList<>();
		}
		rulesteps.add(step);
	}

	public static List<RuleFileName> getAllRuleFiles(Path rootdir, String subjectname) {
		String activedir = getActivePath(false);
		Path path = rootdir.resolve(subjectname).resolve(activedir);
		List<Path> files = new ArrayList<>();
		List<RuleFileName> ret = new ArrayList<>();
		collectFiles(path, path.toFile(), files);
		for (Path p : files) {
			FileTime filedateinactive = null;
			try {
				Path inactivepath = path.resolve(p);
				BasicFileAttributes attr = Files.readAttributes(inactivepath, BasicFileAttributes.class);
				filedateinactive = attr.lastModifiedTime();
			} catch (IOException e) {
			}
			String name = p.toString();
			if (File.separatorChar == '\\') {
				name = name.replace('\\', '/');
			}
			Path activepath = rootdir.resolve(subjectname).resolve(getActivePath(true)).resolve(p);
			FileTime filedateactive = null;
			try {
				BasicFileAttributes activeattr = Files.readAttributes(activepath, BasicFileAttributes.class);
				filedateactive = activeattr.lastModifiedTime();
			} catch (IOException e) {
			}
			RuleFileName n = new RuleFileName(name, subjectname, filedateinactive, filedateactive);
			ret.add(n);
		}
		return ret;
	}

	public static List<RuleFileName> getAllRuleFiles(Path rootdir) {
		File[] subjectnamedirs = rootdir.toFile().listFiles();
		List<RuleFileName> ret = new ArrayList<>();
		for (File f : subjectnamedirs) {
			if (f.isDirectory()) {
				ret.addAll(getAllRuleFiles(rootdir, f.getName()));
			}
		}
		return ret;
	}


	private static void collectFiles(Path startdir, File dir, List<Path> files) {
		File[] filesarray = dir.listFiles();
		if (filesarray != null) {
			for(File f : filesarray) {
				if (f.isDirectory()) {
					collectFiles(startdir, f, files);
				} else {
					Path relative = startdir.relativize(f.toPath());
					files.add(relative);
				}
			}
		}
	}

	public String getSamplefile() {
		return samplefile;
	}

	public void setSamplefile(String samplefile) {
		this.samplefile = samplefile;
	}

	public String getOutputsubjectname() {
		return outputsubjectname;
	}

	public void setOutputsubjectname(String outputsubjectname) {
		this.outputsubjectname = outputsubjectname;
	}

	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	public static void copyToActivate(Path rootdir, String subjectname, String path) throws IOException {
		String inactivedir = getActivePath(false);
		String activedir = getActivePath(true);
		Path sourcepath = rootdir.resolve(subjectname, inactivedir, path);
		Path targetdir = rootdir.resolve(subjectname, activedir);
		Path targetpath = targetdir.resolve(path);
		targetpath.getParent().toFile().mkdirs(); // the filename can have directories too
		Files.copy(sourcepath, targetpath, StandardCopyOption.REPLACE_EXISTING);
	}

	@Override
	public String toString() {
		return getName() + ": RuleFile";
	}

	public void update(RuleFileDefinition empty) {
		if (schema.compareTo(empty.getSchema()) != 0) {
			if (rulesteps != null) {
				for (RuleStep s : rulesteps) {
					if (empty.getRulesteps() != null && empty.getRulesteps().size() > 0) {
						s.update(empty.getRulesteps().get(0));
					}
				}
			}
		}
		setSchema(empty.getSchema());
	}

	public static RuleFileDefinition createEmptyRuleFileDefinition(String subjectname, RulesService service) throws IOException, RestClientException {
		org.apache.avro.Schema schema = service.getLatestSchema(subjectname);
		RuleFileDefinition rg = new RuleFileDefinition(schema);
		rg.setInputsubjectname(subjectname);
		RuleStep step = new RuleStep("next step", schema);
		rg.addRuleStep(step);
		RuleUtils.addRules(step, schema);
		return rg;
	}

	public Schema schema() {
		if (schemadef == null) {
			Schema.Parser p = new Schema.Parser();
			schemadef = p.parse(schema);
		}
		return schemadef;
	}

}

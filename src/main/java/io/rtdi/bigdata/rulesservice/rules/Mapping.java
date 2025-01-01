package io.rtdi.bigdata.rulesservice.rules;

import static java.util.Map.entry;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.jexl3.JexlArithmetic;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.JexlScript;
import org.apache.commons.jexl3.introspection.JexlPermissions;

import io.rtdi.bigdata.rulesservice.PropertiesException;
import io.rtdi.bigdata.rulesservice.jexl.AvroContainer;
import io.rtdi.bigdata.rulesservice.jexl.JexlDefaultFunction;

public abstract class Mapping {

	protected static final JexlEngine jexl = new JexlBuilder().permissions(
			JexlPermissions.parse(
					"io.rtdi.bigdata.rulesservice.jexl.*",
					"java.lang.*",
					"java.math.*",
					"java.text.*",
					"java.util.*",
					"java.time.*",
					"java.lang { Runtime {} System {} ProcessBuilder {} Class {} }",
					"org.apache.commons.jexl3 { JexlBuilder {} }"
					))
			.cache(51200)
			.strict(true)
			.silent(false)
			.namespaces(Map.ofEntries(
					entry("", JexlDefaultFunction.class)
					))
			.create();
	protected JexlScript expression;

	protected Mapping(JexlScript e) {
		this.expression = e;
	}

	protected Mapping(String e) throws IOException {
		this();
		setExpression(e);
	}

	public Mapping() {
		super();
	}

	public Object evaluate(AvroContainer context) throws IOException {
		try {
			return expression.execute(context);
		} catch (JexlException e) {
			StringBuilder msg = createErrorDetails(e);
			throw new PropertiesException("Cannot evaluate the Expression" + msg.toString(), e, "Validate the syntax", expression.getSourceText());
		}
	}

	private StringBuilder createErrorDetails(JexlException e) {
		StringBuilder msg = new StringBuilder();
		if (e.getCause() != e) {
			if (e.getInfo() != null) {
				msg.append("line: ");
				msg.append(e.getInfo().getLine());
				msg.append(", pos: ");
				msg.append(e.getInfo().getColumn());
			}
			msg.append(' ');
			msg.append(e.getDetail());
			Throwable cause = e.getCause();
			if (cause instanceof JexlArithmetic.NullOperand) {
				msg.append(" caused by null operand");
			}
			if (e.getInfo() != null) {
				String[] parts = expression.getSourceText().split("\n");
				if (e.getInfo().getLine()-1 < parts.length) {
					String line = parts[e.getInfo().getLine()-1];
					msg.append(", code segment: ");
					int pos = e.getInfo().getColumn();
					if (pos > 5) {
						pos = pos - 4;
						msg.append("....");
					}
					int end = pos + 20;
					if (end < line.length()) {
						msg.append(line.substring(pos, end));
						msg.append("...");
					} else {
						msg.append(line.substring(pos));
					}
				}
			}
		}
		return msg;
	}

	public void setExpression(String formula) throws IOException {
		try {
			expression = jexl.createScript(formula);
		} catch (JexlException e) {
			StringBuilder msg = createErrorDetails(e);
			throw new PropertiesException("Cannot parse the Expression" + msg.toString(), e, "Validate the syntax", formula);
		}
	}

	public String getExpression() {
		return expression.getSourceText();
	}

	@Override
	public String toString() {
		if (expression != null) {
			return expression.getSourceText();
		} else {
			return "Null-Mapping";
		}
	}

}

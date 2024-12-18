package io.rtdi.bigdata.rulesservice.rules;

import java.io.IOException;

import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.JexlExpression;

import io.rtdi.bigdata.connector.pipeline.foundation.avro.AvroJexlContext;
import io.rtdi.bigdata.rulesservice.PropertiesException;

public abstract class Mapping {

	protected static final JexlEngine jexl = new JexlBuilder().cache(512).strict(true).silent(false).create();
	protected JexlExpression expression;

	protected Mapping(JexlExpression e) {
		this.expression = e;
	}

	protected Mapping(String e) throws IOException {
		this();
		setExpression(e);
	}

	public Mapping() {
		super();
	}

	public Object evaluate(AvroJexlContext context) throws IOException {
		try {
			return expression.evaluate(context);
		} catch (JexlException e) {
			throw new PropertiesException("Cannot evaluate the Expression", e, "Validate the syntax", expression.getSourceText());
		}
	}

	public void setExpression(String formula) throws IOException {
		try {
			expression = jexl.createExpression(formula);
		} catch (JexlException e) {
			throw new PropertiesException("Cannot parse the Expression", e, "Validate the syntax", formula);
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

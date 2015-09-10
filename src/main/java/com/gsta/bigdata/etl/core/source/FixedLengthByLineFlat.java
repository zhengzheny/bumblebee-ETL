package com.gsta.bigdata.etl.core.source;

import java.util.List;
import java.util.Set;

import org.w3c.dom.Element;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.Field;
import com.gsta.bigdata.etl.core.ParseException;

/**
 * source file line's length is fixed,must set refLine
 * 
 * @author shine
 * 
 */
public class FixedLengthByLineFlat extends FixedLengthFlat {
	@JsonIgnore
	private String refLine;
	@JsonIgnore
	private String delimiter;

	private static final String ATTR_REF_LINE = "refLine";
	private static final String DEFAULT_DELIMITER = " ";

	public FixedLengthByLineFlat() {
		super();
	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		super.initAttrs(element);

		this.refLine = super.getAttr(ATTR_REF_LINE);
		if (null == this.refLine || "".equals(this.refLine)) {
			throw new ETLException("the attribute of refLine is no set");
		}

		this.delimiter = super.getAttr(Constants.ATTR_DELIMITER);
		if (null == this.delimiter || "".equals(this.delimiter)) {
			this.delimiter = DEFAULT_DELIMITER;
		}
	}

	@Override
	public void init(Element element) throws ParseException {
		super.init(element);

		List<Field> fields = super.getFields();

		// init reference line,set field's beginPos and endPos
		String[] lines = refLine.split(this.delimiter);
		if (fields.size() != lines.length) {
			throw new ETLException("config file set field count:"
					+ fields.size() + ",but refLine count:" + lines.length);
		}

		int i = 0;
		int pos = 0;
		for (String line : lines) {
			int length = line.length();

			fields.get(i).setBeginPos(pos);
			fields.get(i).setEndPos(pos + length);

			pos = pos + length + 1;
			i++;
		}
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(super.toString());
		sb.append("\r\nrefLine=").append(this.refLine);
		return sb.toString();
	}

	@Override
	public ETLData parseLine(String line, Set<String> invalidRecords)
			throws ETLException, ValidatorException {
		return super.parseLine(line, invalidRecords);
	}
}

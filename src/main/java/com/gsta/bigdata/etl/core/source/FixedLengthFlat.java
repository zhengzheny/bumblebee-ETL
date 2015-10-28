package com.gsta.bigdata.etl.core.source;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Set;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.Field;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.utils.StringUtils;

/**
 * source file line's length is fixed,must set beginPos and endPos attribute
 * 
 * @author shine
 * 
 */
public class FixedLengthFlat extends AbstractSourceMetaData {
	private static final long serialVersionUID = 4769367775777755639L;
	@JsonProperty
	private int lineLength = -1;
	@JsonProperty
	private String encoding;

	private static final String ATTR_LINE_LENGTH = "lineLength";
	private static final String ATTR_ENCODING = "encoding";
	private static final String DEFAULT_ENCODING = "GBK";
	private static final String ISO_ENCODING = "ISO-8859-1";
	private static final String APPOINT_CLASS = "FixedLengthFlat";

	public FixedLengthFlat() {
		super();
	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		super.initAttrs(element);

		this.encoding = super.getAttr(ATTR_ENCODING);
		if (null == this.encoding || "".equals(this.encoding)) {
			this.encoding = DEFAULT_ENCODING;
		}

		String strLineLength = super.getAttr(ATTR_LINE_LENGTH);
		if(strLineLength != null && !"".equals(strLineLength))
		{
			this.lineLength = Integer.parseInt(strLineLength);
		} 
	}

	@Override
	protected void createChildNode(Element node) throws ParseException {
		super.createChildNode(node);
	}

	@Override
	protected void createChildNodeList(NodeList nodeList) throws ParseException {
		super.createChildNodeList(nodeList);
	}

	@Override
	public void init(Element element) throws ParseException {
		super.init(element);

		List<Field> fields = super.getFields();
		// validate field whether have length
		int pos = 0;
		for (Field field : fields) {
			int length = field.getLength();
			
			// exclude subclass validate the length attribute
			if (getClass().getSimpleName().equals(APPOINT_CLASS)
					&& length == -1) {
				throw new ETLException("field:" + field.getId()
						+ " must set the attribute of length");
			}
			
			if (length != -1) {
				field.setBeginPos(pos);
				field.setEndPos(pos + length);
				pos = pos + length + 1;
			}
		}
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(super.toString());
		sb.append("\r\nlineLength=").append(this.lineLength);
		return sb.toString();
	}

	@Override
	public ETLData parseLine(String line, Set<String> invalidRecords)
			throws ETLException, ValidatorException {
		Preconditions.checkNotNull(line, "data line is null");
		
		if (line.trim().length() == 0) {
			throw new ETLException(ETLException.NULL_LINE_TRIM,"data line is null");
		}

		int lineLen = 0;
		int trimRightLineLen = 0;
		String tempLine = "";
		try {
			lineLen = line.getBytes(this.encoding).length;
			byte[] bytes = StringUtils.rightTrim(line).getBytes(this.encoding);
			trimRightLineLen = bytes.length;
			tempLine = new String(bytes, ISO_ENCODING);
		} catch (UnsupportedEncodingException e) {
			throw new ETLException(ETLException.UNSUPPORTED_ENCODING,"the encoding:" + this.encoding
					+ " is not supported");
		}

		if (this.lineLength != -1 && this.lineLength != lineLen) {
				throw new ETLException(ETLException.DATA_NOT_EQUAL_DEFINITION,
						"data line is not equal <sourceMetaData> lineLength's value");
		}

		List<Field> fields = super.getFields();
		int lastEndPos = fields.get(fields.size() - 1).getEndPos();
		if (lastEndPos != trimRightLineLen) {
			throw new ETLException(ETLException.DATA_LENGTH_NOT_EQUAL_LAST_POS,
					"input line is not equal the config set last field endPos's value");
		}

		String[] data = this.splitLine(tempLine, fields);

		ETLData etlData = new ETLData();
		for (int i = 0; i < data.length; i++) {
			Field field = fields.get(i);

			super.fieldValidate(field, data[i], line, invalidRecords);

			etlData.addData(field.getId(), data[i]);
		}

		return etlData;
	}

	// split line by beginPos and endPos
	private String[] splitLine(String line, List<Field> fields) {
		if(fields == null || fields.size() <= 0){
			return new String[0];
		}
		
		int size = fields.size();
		String[] result = new String[size];
		for (int i = 0; i < size; i++) {
			Field field = fields.get(i);
			
			int beginPos = field.getBeginPos();
			int endPos = field.getEndPos();
			String data = line.substring(beginPos, endPos).trim();
			
			try {
				result[i] = new String(data.getBytes(ISO_ENCODING),this.encoding);
			} catch (UnsupportedEncodingException e) {
				throw new ETLException(ETLException.UNSUPPORTED_ENCODING,"the encoding:" + this.encoding
						+ " is not supported");
			}
		}
		
		return result;
	}
}

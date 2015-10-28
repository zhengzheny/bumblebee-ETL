package com.gsta.bigdata.etl.core.source;

import java.util.Set;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.Field;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.utils.StringUtils;

/**
 * source file is flat file,default delimiter="|"
 * 
 * @author tianxq
 * 
 */
public class SimpleFlat extends AbstractSourceMetaData {
	private static final long serialVersionUID = 4500447521341109479L;
	@JsonProperty
	private String delimiter = "\\|";

	public SimpleFlat() {
		super();
	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		super.initAttrs(element);
		this.delimiter = super.getAttr(Constants.ATTR_DELIMITER);
	}

	@Override
	protected void createChildNode(Element node) throws ParseException {
		super.createChildNode(node);
	}

	@Override
	protected void createChildNodeList(NodeList nodeList) throws ParseException {
		super.createChildNodeList(nodeList);
	}

	public String getDelimiter() {
		return delimiter;
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(super.toString());
		sb.append("\r\ndelimiter=").append(this.delimiter);
		return sb.toString();
	}

	@Override
	public ETLData parseLine(String line,
			Set<String> invalidRecords) throws ETLException,ValidatorException {
		Preconditions.checkNotNull(line, "data line is null");

		String[] data = StringUtils.splitByWrapper(line, 
				this.delimiter,super.getWrapper());
		
		if(data == null){
			throw new ETLException(ETLException.NULL_DATA_BY_SPLIT,"parse line data occur null,delimiter=" +
		        this.delimiter + ",wrapper=" + super.getWrapper());
		}
		
		if (data.length != super.getFields().size()) {
			throw new ETLException(ETLException.DATA_NOT_EQUAL_DEFINITION,"data " + line + " record count="
					+ data.length + ",but source definition field count="
					+ super.getFields().size());
		}

		ETLData etlData = new ETLData();
		for (int i = 0; i < data.length; i++) {
			Field field = super.getFields().get(i);

			etlData.addData(field.getId(), data[i]);
			
			super.fieldValidate(field, data[i], line, invalidRecords);
		}

		return etlData;
	}
}

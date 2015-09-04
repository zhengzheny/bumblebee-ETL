package com.gsta.bigdata.etl.core;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.xml.xpath.XPathExpressionException;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.utils.XmlTools;
/**
 * 
 * @author tianxq
 *
 */
public class OutputMetaData extends AbstractETLObject {
	@JsonProperty
	private String outputPath;
	@JsonProperty
	private String errorPath; // save error record
	@JsonProperty
	private String charset;
	@JsonProperty
	private String fileSuffix;
	@JsonProperty
	private String valuesDelimiter = "\\|";
	protected static final String NotSeeCharDefineInConf = "001";
	@JsonProperty
	private List<String> valuesFields = new ArrayList<String>();

	public OutputMetaData() {
		super.tagName = Constants.PATH_OUTPUT_METADATA;

		super.registerChildrenTags(new ChildrenTag(
				Constants.PATH_OUTPUT_METADATA_VALUES, ChildrenTag.NODE));
		super.registerChildrenTags(new ChildrenTag(
				Constants.PATH_OUTPUT_METADATA_VALUES_FIELD,
				ChildrenTag.NODE_LIST));
	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		this.outputPath = super.getAttr(Constants.ATTR_OUTPUT_PATH);
		if (this.outputPath == null) {
			throw new ParseException("output path is null");
		}

		if (this.outputPath.endsWith("/") || this.outputPath.endsWith("\\")) {
			this.outputPath = this.outputPath.substring(0,
					this.outputPath.length() - 1);
		}

		this.errorPath = super.getAttr(Constants.ATTR_ERROR_PATH);
		if (this.errorPath == null || "".equals(this.errorPath)) {
			// don't use File.separator,or don't pass test in windows platform
			this.errorPath = this.outputPath + "/error";
		}

		this.charset = super.getAttr(Constants.ATTR_CHARSET, "utf-8");
		this.fileSuffix = super.getAttr(Constants.ATTR_FILE_SUFFIX, "txt");
	}

	@Override
	protected void createChildNode(Element node) throws ParseException {
		Preconditions.checkNotNull(node, "element is null");

		// get values delimiter
		if (node.getNodeName().equals(Constants.PATH_OUTPUT_METADATA_VALUES)) {
			try {
				String delimiter = XmlTools.getNodeAttr(node,
						Constants.ATTR_DELIMITER);
				this.valuesDelimiter = Context.getValue(delimiter);
				// special deal with not see char
				if (NotSeeCharDefineInConf.equals(this.valuesDelimiter)) {
					this.valuesDelimiter = "\001";
				}
			} catch (XPathExpressionException e) {
				throw new ParseException(e);
			}
		}
	}

	@Override
	protected void createChildNodeList(NodeList nodeList) throws ParseException {
		Preconditions.checkNotNull(nodeList, "nodeList is null");

		for (int i = 0; i < nodeList.getLength(); i++) {
			Node element = nodeList.item(i);
			if (element.getNodeType() == Node.ELEMENT_NODE
					&& element.getNodeName().equals(Constants.TAG_FIELD)) {
				createValuesField(element);
			}
		}
	}

	private void createValuesField(Node element) {
		// values
		if (element.getParentNode().getNodeName()
				.matches(Constants.PATH_OUTPUT_METADATA_VALUES)) {
			try {
				String field = XmlTools.getNodeAttr((Element) element,
						Constants.ATTR_ID);
				field = Context.getValue(field);

				this.valuesFields.add(field);
			} catch (XPathExpressionException e) {
				throw new ParseException(e);
			}
		}
	}

	@JsonIgnore
	public String getOutputValue(ETLData data) throws ETLException{
		Preconditions.checkNotNull(data, "input data is null");

		//if get null field,throws ETLException and report all null field name
		boolean nullFlag=false;
		String nullFieldNames = "";
		
		// * means output all fields
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < this.valuesFields.size(); i++) {
			String strField = this.valuesFields.get(i);
			// if field is *,means add all source fields to output
			if ("*".equals(strField)) {
				Iterator<String> iter = data.getFieldNames().iterator();
				while (iter.hasNext()) {
					String fieldName = iter.next();
					String dataValue = data.getData().get(fieldName);
					sb.append(dataValue).append(this.valuesDelimiter);
					if(dataValue == null){
						nullFlag = true;
						nullFieldNames = nullFieldNames + fieldName + ",";
					}
				}
			} else {
				String dataValue = data.getData().get(strField);
				sb.append(dataValue).append(this.valuesDelimiter);
				if(dataValue == null){
					nullFlag = true;
					nullFieldNames = nullFieldNames + strField + ",";
				}
			}
		}
		
		if(nullFlag){
			throw new ETLException(ETLException.NULL_FIELD_NAMES,nullFieldNames);
		}

		String ret = sb.toString();
		if (ret.endsWith(this.valuesDelimiter)) {
			ret = ret.substring(0, ret.length() - this.valuesDelimiter.length());
		}

		return ret;
	}

	public String getOutputPath() {
		return outputPath;
	}

	public String getErrorPath() {
		return errorPath;
	}

	public String getCharset() {
		return charset;
	}

	public String getFileSuffix() {
		return fileSuffix;
	}

	public String getValuesDelimiter() {
		return valuesDelimiter;
	}
	
	public List<String> getValuesFields() {
		return valuesFields;
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("\r\noutputPath=").append(this.outputPath);
		sb.append("values delmiter=").append(this.valuesDelimiter);
		sb.append("\r\nvalues field=");
		sb.append(this.valuesFields.toString());

		return sb.toString();
	}

}

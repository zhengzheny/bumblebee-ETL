package com.gsta.bigdata.etl.mapreduce;

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
import com.gsta.bigdata.etl.core.ChildrenTag;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.Context;
import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.OutputMetaData;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.utils.XmlTools;
/**
 * 
 * @author tianxq
 *
 */
public class MROutputMetaData extends OutputMetaData  {
	@JsonProperty
	private String keysDelimiter = "\\|";
	@JsonProperty
	private List<String> keysFields = new ArrayList<String>();
	
	public MROutputMetaData() {
		super();
		
		super.registerChildrenTags(new ChildrenTag(
				Constants.PATH_MAP_OUTPUT_METADATA_KEYS,ChildrenTag.NODE));
		super.registerChildrenTags(new ChildrenTag(
				Constants.PATH_MAP_OUTPUT_METADATA_KEYS_FIELD,ChildrenTag.NODE_LIST));
	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		super.initAttrs(element);
	}

	@Override
	protected void createChildNode(Element node) throws ParseException {
		Preconditions.checkNotNull(node, "element is null");
		
		super.createChildNode(node);
		
		//get keys delimiter
		if(node.getNodeName().equals(Constants.PATH_MAP_OUTPUT_METADATA_KEYS)){
			try {
				String delimiter = XmlTools.getNodeAttr(node, Constants.ATTR_DELIMITER);
				this.keysDelimiter = Context.getValue(delimiter);
				//special deal with not see char
				if(NotSeeCharDefineInConf.equals(this.keysDelimiter)){
					this.keysDelimiter = "\001";
				}
			} catch (XPathExpressionException e) {
				throw new ParseException(e);
			}
		}
	}

	@Override
	protected void createChildNodeList(NodeList nodeList)
			throws ParseException {
		Preconditions.checkNotNull(nodeList, "nodeList is null");
		
		super.createChildNodeList(nodeList);
		
		for(int i=0;i<nodeList.getLength();i++)
		{
			Node element = nodeList.item(i);
			if(element.getNodeType() == Node.ELEMENT_NODE && 
					element.getNodeName().equals(Constants.TAG_FIELD)){
				createKeysField(element);
			}
		}
	}
	
	private void createKeysField(Node element){
		// keys
		if (element.getParentNode().getNodeName()
				.matches(Constants.PATH_MAP_OUTPUT_METADATA_KEYS)) {
			try {
				String field = XmlTools.getNodeAttr((Element) element,Constants.ATTR_ID);
				field = Context.getValue(field);
				
				this.keysFields.add(field);
			} catch (XPathExpressionException e) {
				throw new ParseException(e);
			}
		}
	}
	
	@JsonIgnore
	public String getOutputKey(ETLData data)
			throws ETLException{
		Preconditions.checkNotNull(data, "input data is null");
		
		// if get null field,throws ETLException and report all null field name
		boolean nullFlag = false;
		String nullFieldNames = "";
				
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < this.keysFields.size(); i++) {
			String strField = this.keysFields.get(i);
			//if field is *,means add all source fields to output 
			if ("*".equals(strField)) {
				Iterator<String> iter = data.getFieldNames().iterator();
				while(iter.hasNext()){
					String fieldName = iter.next();
					String dataValue = data.getData().get(fieldName);
					sb.append(dataValue).append(this.keysDelimiter);
					
					if(dataValue == null){
						nullFlag = true;
						nullFieldNames = nullFieldNames + fieldName + ",";
					}
				}
			} else {
				String dataValue = data.getData().get(strField);
				sb.append(dataValue).append(this.keysDelimiter);
				
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
		if(ret.endsWith(this.keysDelimiter)){
			ret = ret.substring(0,ret.length() - this.keysDelimiter.length());
		}
		
		return ret;
	}
	
	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append(super.toString());
				
		sb.append("\r\nkeys delimiter=").append(this.keysDelimiter);
		sb.append("\r\nkeys field=");
		sb.append(this.keysFields.toString());
		
		return sb.toString();
	}
}

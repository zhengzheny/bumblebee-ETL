package com.gsta.bigdata.etl.core;

import java.util.Properties;

import javax.xml.xpath.XPathExpressionException;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.gsta.bigdata.utils.XmlTools;
/**
 * 
 * @author tianxq
 * 
 */
public class ComputingFrameworkConfigs extends AbstractETLObject {
	@JsonProperty
	private Properties conf = new Properties();

	public ComputingFrameworkConfigs() {
		super.tagName = Constants.PATH_COMPUTING_FRAMEWORK_CONFIGS;
		
		super.registerChildrenTags(new ChildrenTag(Constants.PATH_COMPUTING_FRAMEWORK_CONFIGS_PROPERTY,
				ChildrenTag.NODE_LIST));
	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		// no attribute

	}

	@Override
	protected void createChildNode(Element node) throws ParseException {
		// no single child node

	}

	@Override
	protected void createChildNodeList(NodeList nodeList) throws ParseException {
		Preconditions.checkNotNull(nodeList, "element is null");
		
		for(int i=0;i<nodeList.getLength();i++){
			Node node = nodeList.item(i);
			if(node.getNodeType() == Node.ELEMENT_NODE){
				try {
					String key = XmlTools.getNodeAttr((Element)node, Constants.ATTR_KEY);
					String value = XmlTools.getNodeAttr((Element)node, Constants.ATTR_VALUE);
					
					key = Context.getValue(key);
					value = Context.getValue(value);
					
					conf.put(key, value);
				} catch (XPathExpressionException e) {
					throw new ParseException(e);
				}
			}//end if
		}
	}
	
	@JsonIgnore
	public String getCFConf(String key){
		return conf.getProperty(key);
	}
	
	@JsonIgnore
	public String getCFConf(String key,String defaultValue){
		String value = conf.getProperty(key);
		if(null == value || "".equals(value)){
			value = defaultValue;
		}
		
		return value;
	}
	
	public String toString(){
		return conf.toString();
	}
}

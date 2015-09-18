package com.gsta.bigdata.etl.core.lookup;

import java.util.HashMap;
import java.util.Map;

import javax.xml.xpath.XPathExpressionException;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.gsta.bigdata.etl.core.AbstractETLObject;
import com.gsta.bigdata.etl.core.ChildrenTag;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ContextMgr;
import com.gsta.bigdata.etl.core.LoadException;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.utils.XmlTools;

/**
 * 
 * @author Shine
 * 
 */

public abstract class AbstractDataSource extends AbstractETLObject {
	@JsonIgnore
	private String type;
	@JsonIgnore
	private String id;
	@JsonIgnore
	private String ref;
	@JsonIgnore
	Map<String, String> properties = new HashMap<String, String>();

	public AbstractDataSource() {
		super.tagName = Constants.PATH_LKP_DATASOURCE;
		
		super.registerChildrenTags(new ChildrenTag(
				Constants.PATH_LKP_DATASOURCE_PROPERTY, ChildrenTag.NODE_LIST));
		super.registerChildrenTags(new ChildrenTag(
				Constants.PATH_LKP_DATASOURCE_FIELDS, ChildrenTag.NODE));
	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		this.id = super.getAttr(Constants.ATTR_ID);
		this.ref = super.getAttr(Constants.ATTR_REF);
		this.type = super.getAttr(Constants.ATTR_TYPE);

		if (null == this.type || "".equals(this.type)) {
			this.type = Constants.DEFAULT_LKP_DS_TYPE;
		}
	}

	@Override
	protected void createChildNode(Element node) throws ParseException {
		//has no child node
	}

	@Override
	protected void createChildNodeList(NodeList nodeList) throws ParseException {
		Preconditions.checkNotNull(nodeList, "nodeList is null");

		for (int i = 0; i < nodeList.getLength(); i++) {
			Node element = nodeList.item(i);
			if (element.getNodeType() == Node.ELEMENT_NODE
					&& element.getNodeName().equals(Constants.PATH_LKP_DATASOURCE_PROPERTY)) {
				try {
					String name = XmlTools.getNodeAttr((Element) element,Constants.ATTR_NAME);
					String value = XmlTools.getNodeAttr((Element) element,Constants.ATTR_VALUE);
					
					value = ContextMgr.getValue(value);
					this.properties.put(name, value);
				} catch (XPathExpressionException e) {
					throw new ParseException(e);
				}
			}
			
		}

		if (this.properties.size() <= 0) {
			throw new ParseException(
					"datasource must have one or more properties");
		}

	}
	
	public abstract Map<String,String> load(String key, String value) throws LoadException;

	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("id=").append(this.id);
		sb.append("\r\n type:=").append(this.type);
		sb.append("\r\n properties=").append(this.properties);

		return sb.toString();
	}

	public String getType() {
		return type;
	}

	public String getId() {
		return id;
	}

	public String getRef() {
		return ref;
	}

	public Map<String, String> getProperties() {
		return properties;
	}
	
}

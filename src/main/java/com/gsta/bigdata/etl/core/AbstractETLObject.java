package com.gsta.bigdata.etl.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.xpath.XPathExpressionException;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.gsta.bigdata.utils.XmlTools;

/**
 * the base class for all etl model 
 * when parsing xml file to etl model.
 * 
 * 0.explicitly define tag name in subclass
 * 1.add childrenTag in subclass construct function
 * 3.if node has attributes,please parse attribute in subclass
 * 4.if child node is single node,please createChildNode in subclass
 * 5.if child node is node list,please createChildNodeList in subclass
 * 
 * @author tianxq
 *
 */
public abstract class AbstractETLObject implements Serializable{
	private static final long serialVersionUID = -3816609682547583774L;

	//node tag name
	@JsonIgnore
	protected String tagName = "";
	
	//node's children tag,initialize in subclass construct function
	@JsonIgnore
	private List<ChildrenTag> childrenTags = new ArrayList<ChildrenTag>();
	
	//object attributes
	@JsonProperty
	private Map<String,String> attrs= new HashMap<String,String>();
	
	public AbstractETLObject() {
		
	}
	
	/**
	 * init element attributes and children node
	 * @param element
	 * @throws ParseException
	 */
	public void init(Element element) throws ParseException{
		Preconditions.checkNotNull(element, "element " + tagName + " is null");
		
		if(!tagName.equals(element.getNodeName())){
			throw new ParseException("invalid "+element.getNodeName() +" element,expected tag name:" + tagName);
		}
		
		buildAttrs(element);
		
		//abstract function
		initAttrs(element);
		
		initChildrenNodes(element);
	}

	protected abstract void initAttrs(Element element) throws ParseException;
	
	private void buildAttrs(Element element){
		this.attrs = XmlTools.getNodeAttrs(element);
		
		//read config variable from *.properties file and shell command line
		Set<String> keys = this.attrs.keySet();
		Iterator<String> iter = keys.iterator();
		while (iter.hasNext()) {
			String key = iter.next();
			String value = this.attrs.get(key);

			// update
			this.attrs.put(key,ContextMgr.getValue(value));
		}
	}
		
	private void initChildrenNodes(Element element) throws ParseException{
		Iterator<ChildrenTag> iter = childrenTags.iterator();
		while(iter.hasNext()){
			ChildrenTag childrenTag = (ChildrenTag)iter.next();
			if(childrenTag.getType() == ChildrenTag.NODE){
				Element childNode = XmlTools.getFirstChildByTagName(element, childrenTag.getPath());
				//if don't define,ignore child node
				if(null != childNode){
					createChildNode(childNode);
				}
			}else if(childrenTag.getType() == ChildrenTag.NODE_LIST){
				try {
					NodeList childNodeList = XmlTools.getNodeListByPath(element, childrenTag.getPath());
					//if don't define,ignore child node list
					if(null != childNodeList){
						createChildNodeList(childNodeList);
					}
				} catch (XPathExpressionException e) {
					throw new ParseException(e);
				}				
			}else{
				throw new ParseException("invalid child node type=" + childrenTag.getType());
			}
		}
	}
	
	protected abstract void createChildNode(Element node) throws ParseException;
	
	protected abstract void createChildNodeList(NodeList nodeList) throws ParseException;
	
	protected void registerChildrenTags(ChildrenTag childrenTag){
		if(null != childrenTag){
			childrenTags.add(childrenTag);
		}
	}
	
	@JsonIgnore
	protected String getAttr(String attrName){
		return this.attrs.get(attrName);
	}
	
	@JsonIgnore
	protected String getAttr(String attrName,String defaultValue){
		String ret = this.attrs.get(attrName);
		if(ret == null || "".equals(ret)){
			return defaultValue;
		}
		
		return ret;
	}
}
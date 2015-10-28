package com.gsta.bigdata.etl.core;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.gsta.bigdata.etl.ETLException;

/**
 * 
 * @author tianxq
 *
 */
public class Transforms extends AbstractETLObject {
	private static final long serialVersionUID = -342179632071115871L;
	@JsonProperty
	private List<Transform> transforms = new ArrayList<Transform>();
	
	public Transforms() {
		super.tagName = Constants.PATH_TRANSFORMS;
		
		super.registerChildrenTags(new ChildrenTag(
				Constants.PATH_TRANSFORM, ChildrenTag.NODE_LIST));
	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		//has no attribute

	}

	@Override
	protected void createChildNode(Element node) throws ParseException {
		//has no single child node
		
	}

	@Override
	protected void createChildNodeList(NodeList nodeList) throws ParseException {
		Preconditions.checkNotNull(nodeList, "nodeList is null");
		
		for(int i=0;i<nodeList.getLength();i++)
		{
			Node element = nodeList.item(i);
			if(element.getNodeType() == Node.ELEMENT_NODE && 
					element.getNodeName().equals(Constants.PATH_TRANSFORM)){
				Transform transform = new Transform();
				transform.init((Element)element);
				
				this.transforms.add(transform);
			}
		}

	}
	
	/**
	 * etl transfrom
	 * @param data - data line
	 * @param context
	 * @throws ETLException
	 */
	public void onTransform(ETLData data,ShellContext context) throws ETLException {
		Iterator<Transform> iter = this.transforms.iterator();
		while (iter.hasNext()) {
			Transform transform = iter.next();
			transform.onTransform(data, context);
		}
	}

	public String toString(){
		StringBuffer sb = new StringBuffer();
		Iterator<Transform> iter = this.transforms.iterator();
		while(iter.hasNext()){
			sb.append(iter.next().toString()).append("\r\n");
		}
		
		return sb.toString();
	}
}

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
import com.gsta.bigdata.etl.core.filter.AbstractFilter;
import com.gsta.bigdata.etl.core.function.AbstractFunction;

/**
 * transform have zero or more filters and one or more functions.
 * if filters accept,then will calculate function.
 * 
 * if there are different functions have different filter,
 * please set different transform.
 * 
 * @author tianxq
 *
 */
public class Transform extends AbstractETLObject {
	@JsonProperty
	private List<AbstractFunction> functions = new ArrayList<AbstractFunction>();
	@JsonProperty
	private List<AbstractFilter> filters = new ArrayList<AbstractFilter>();
	@JsonProperty
	private List<String> scopes = new ArrayList<String>();
	
	public Transform() {
		super.tagName = Constants.PATH_TRANSFORM;
		
		super.registerChildrenTags(new ChildrenTag(
				Constants.PATH_TRANSFORM_FUNCTION,ChildrenTag.NODE_LIST));
		super.registerChildrenTags(new ChildrenTag(
				Constants.PATH_TRANSFORM_FILTER,ChildrenTag.NODE_LIST));
	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		String strScope = super.getAttr(Constants.ATTR_SCOPE);
		
		this.initScope(strScope);
	}
	
	private void initScope(String scope){
		if(null == scope || "".equals(scope)){
			scopes.add(Constants.DEFAULT_TRANSFORM_SCOPE);
			return;
		}
		
		String[] tempScopes = scope.split(",");
		if(null == tempScopes || tempScopes.length == 0){
			scopes.add(Constants.DEFAULT_TRANSFORM_SCOPE);
			return;
		}
		
		for(String tempScope:tempScopes){
			scopes.add(tempScope);
		}
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
			//create function
			if(element.getNodeType() == Node.ELEMENT_NODE && 
					element.getNodeName().equals(Constants.PATH_TRANSFORM_FUNCTION)){
				AbstractFunction function = AbstractFunction.newInstance((Element)element);
				function.init((Element)element);
				
				this.functions.add(function);
			}
			
			//create filter		
			if(element.getNodeType() == Node.ELEMENT_NODE && 
					element.getNodeName().equals(Constants.PATH_TRANSFORM_FILTER)){
				AbstractFilter filter = AbstractFilter.newInstance((Element)element);
				filter.init((Element)element);
				
				this.filters.add(filter);
			}
		}
		
		if(this.functions.size() <= 0){
			throw new ParseException("transform must have one or more functions.");
		}
	}

	/**
	 * etl transfrom
	 * @param data
	 */
	protected void onTransform(ETLData data, ShellContext context)
			throws ETLException {
		boolean accept = true;
		String filterName = null;

		// filter
		Iterator<AbstractFilter> filterIter = this.filters.iterator();
		while (filterIter.hasNext()) {
			AbstractFilter filter = filterIter.next();
			
			if (!filter.filter(data,context)) {
				accept = false;
				filterName = filter.getName();
				break;
			}
		}

		//filter failure,write error file,don't write output file
		if (!accept) {
			throw new ETLException(ETLException.FILTER_ACCEPT,"filter by " + filterName);
		}
		
		// function
		Iterator<AbstractFunction> functionIter = this.functions.iterator();
		while (functionIter.hasNext()) {
			functionIter.next().getResult(data.getData(),context);
		}
	}

	public boolean hasScope(String scope){
		return scopes.contains(scope);
	}

	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append("scopes=").append(this.scopes);
		sb.append("\r\nfunctions:\r\n").append(this.functions);
		sb.append("\r\nfilters:\r\n").append(this.filters);
		
		return sb.toString();
	}
}

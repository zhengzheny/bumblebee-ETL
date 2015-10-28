package com.gsta.bigdata.etl.core.function.model;

import java.util.ArrayList;
import java.util.List;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.core.AbstractETLObject;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.etl.core.function.AbstractRedisFunc;

/**
 * redis key inner class
 * json don't support inner class,so put it into function.model package
 *
 */
public class RedisKey extends AbstractETLObject {
	private static final long serialVersionUID = 954360544257397618L;
	@JsonProperty
	private List<String> fields = new ArrayList<String>();
	@JsonProperty
	private String delimiter = "+";
	@JsonProperty
	private int monthMode = 3;
	
	public final static String ATTR_FIELDS = "fields";
	public final static String ATTR_MONTH_MODE = "monthMode";
	
	public RedisKey(){
		super();
		super.tagName = AbstractRedisFunc.PATH_KEY;
	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		String strFields = super.getAttr(ATTR_FIELDS);
		String[] arrFields = strFields.split(",");
		for(String field:arrFields){
			this.fields.add(field);
		}
		
		String str = super.getAttr(Constants.ATTR_DELIMITER);
		if(str != null && !"".equals(str)){
			this.delimiter = str;
		}
		
		str = super.getAttr(ATTR_MONTH_MODE);
		if(str != null && !"".equals(str)){
			this.monthMode = Integer.parseInt(str);
		}
	}

	@Override
	protected void createChildNode(Element node) throws ParseException {
		//has no child 
	}

	@Override
	protected void createChildNodeList(NodeList nodeList)
			throws ParseException {
		//has no child list
	}

	public List<String> getFields() {
		return fields;
	}

	public String getDelimiter() {
		return delimiter;
	}

	public int getMonthMode() {
		return monthMode;
	}
}
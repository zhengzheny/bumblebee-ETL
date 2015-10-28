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
 * redis value inner class
 *
 */
public class RedisValue extends AbstractETLObject {
	private static final long serialVersionUID = 486993402652325945L;
	@JsonProperty
	private List<String> fields = new ArrayList<String>();
	@JsonProperty
	private String delimiter = "+";
	
	public final static String ATTR_FIELDS = "fields";
	
	public RedisValue(){
		super();
		super.tagName = AbstractRedisFunc.PATH_VALUE;
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
}
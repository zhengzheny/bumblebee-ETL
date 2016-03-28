package com.gsta.bigdata.etl.core.lookup;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;


/**
 * lookup interface
 * 
 * @author shine
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
@JsonSubTypes({ @JsonSubTypes.Type(value = LKPTable.class, name = "LKPTable")})
public interface ILookup {
	/**
	 * find value by key
	 * @param key
	 */
	public Object getValue(String key);

	/**
	 * judge key is or not exist
	 * @param key
	 */
	public boolean isExist(String key);
	

	/**
	 * if isReverse is true,get key by value
	 * @param key
	 * @param isReverse
	 */
	public Object getValue(String key, boolean isReverse);

}
package com.gsta.bigdata.utils;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 
 * @author tianxq
 *
 */
public class BeansUtils {
	/**
	 * transform class to json string in configuration
	 * @param key
	 * @param conf
	 * @param object
	 * @throws JsonProcessingException 
	 */
	public static String obj2json(Object object) throws JsonProcessingException {
		ObjectMapper om = new ObjectMapper();
		return om.writeValueAsString(object);
	}
	
	/**
	 * transform json string to class
	 * @param key
	 * @param conf
	 * @param clazz-  Object.class
	 * @return
	 * @throws IOException 
	 * @throws JsonMappingException 
	 * @throws JsonParseException 
	 */
	public static <T> T json2obj(String json, Class<T> clazz) throws JsonParseException, JsonMappingException, IOException {
		if(json == null || "".equals(json)){
			return null;
		}
		
		ObjectMapper om = new ObjectMapper();
		om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		return om.readValue(json, clazz);
	}
}

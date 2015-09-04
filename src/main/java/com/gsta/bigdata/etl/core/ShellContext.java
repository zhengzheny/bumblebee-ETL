package com.gsta.bigdata.etl.core;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * etl context
 * 	you can set context from main entry,for example:
 * ETLRunner configFile=/etl.xml processId=ex1 month=1
 * 	get value from context in process:
 * context.getValue("month","-1");
 * then get value is 1. 
 * @author tianxq
 *
 */
public class ShellContext {
	@JsonProperty
	private Map<String, String> context = new HashMap<String, String>();
	private final static ShellContext etlContext = new ShellContext();
	
	public void parseArgs(String[] args) {
		if (args == null || args.length == 0) {
			return;
		}

		for (String arg : args) {
			int index = arg.indexOf("=");
			if (index >= 0) {
				String key = arg.substring(0, index);
				String value = arg.substring(index + 1, arg.length());
				this.context.put(key, value);
			}
		}
	}

	@JsonIgnore
	public String getValue(String key, String defaultValue) {
		if(this.context.containsKey(key)){
			return this.context.get(key);
		}else{
			return defaultValue;
		}
	}
	
	@JsonIgnore
	public String getValue(String key) {
		if(this.context.containsKey(key)){
			return this.context.get(key);
		}
		
		return key;
	}
	
	public String toString(){
		return this.context.toString();
	}
	
	public static ShellContext getInstance(){
		return etlContext;
	}
}

package com.gsta.bigdata.etl.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * store etl source data
 * 
 * @author Shine
 * 
 *
 */
public class ETLData implements Serializable{
	private static final long serialVersionUID = -4057144719064581319L;
	private Map<String, String> data ;
	private List<String> fieldNames ;
	
	public ETLData(){
		data = new HashMap<String, String>();
		fieldNames = new ArrayList<String>();	
	}

	public void addData(String fieldName, String dataValue) {
		this.data.put(fieldName, dataValue);
		this.fieldNames.add(fieldName);
	}
	
	public void addData(ETLData data){
		if(data != null){
			for(Map.Entry<String, String> entry:data.getData().entrySet()){
				this.addData(entry.getKey(), entry.getValue());
			}
		}
	}
	
	public void addData(HashMap<String, String> map){
		this.data.putAll(map);
		this.fieldNames.addAll(map.keySet());
	}
	
	public String getValue(String key){
		return this.data.get(key);
	}

	public Map<String, String> getData() {
		return data;
	}

	public List<String> getFieldNames() {
		return fieldNames;
	}

	public void clear(){
		data.clear();
		fieldNames.clear();
	}
	
	public String toString(){
		return this.data.toString();
	}
}

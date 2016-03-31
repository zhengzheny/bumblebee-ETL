package com.gsta.bigdata.etl.core.lookup;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.core.source.mro.DIMObj;

public class DIMTable extends AbstractLKPTable {
	private static final long serialVersionUID = 8738059030705233503L;
	@JsonProperty
	
	private Map<String, DIMObj> dimensions = new HashMap<String, DIMObj>();
	
	public DIMTable() {
		super();
	}

	@Override
	public void load() {
		if (super.dataSource == null) {
			return;
		}

		String key = super.tableMaps.firstKey();
		Map<String, Object> map = super.dataSource.load(key, null);
		
		for (Map.Entry<String, Object> entry : map.entrySet()) {
			DIMObj dimObj = (DIMObj) entry.getValue();
			this.dimensions.put(entry.getKey(), dimObj);
		}
	}
	
	/**
	 * 
	 * @param key
	 */
	@Override
	public Object getValue(String key) {
		if (this.dimensions != null) {
			return this.dimensions.get(key);
		}

		return null;
	}
	
	/**
	 * 
	 * @param key
	 */
	@Override
	public boolean isExist(String key) {
		if (this.dimensions != null) {
			return this.dimensions.containsKey(key);
		}

		return false;
	}

	@Override
	public Object getValue(String key, boolean isReverse) {
		//don't need reverse search
		return null;
	}

	/*public Map<String, DIMObj> getDimensions() {
		return this.dimensions;
	}*/
}

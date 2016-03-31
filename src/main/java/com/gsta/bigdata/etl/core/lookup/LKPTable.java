package com.gsta.bigdata.etl.core.lookup;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.core.ContextMgr;

/**
 * @author shine
 */
public class LKPTable extends AbstractLKPTable{
	private static final long serialVersionUID = 1950990165957160536L;
	@JsonProperty
	private Map<String, String> reverseDimensions = new HashMap<String, String>();
	@JsonProperty
	private Map<String, String> dimensions = new HashMap<String, String>();

	public LKPTable() {
		super();
	}

	/**
	 * 
	 * @param key
	 * @param isReverse
	 */
	public Object getValue(String key, boolean isReverse) {
		if (isReverse) {
			if (this.reverseDimensions != null) {
				return this.reverseDimensions.get(key);
			}
		} else {
			if (this.dimensions != null) {
				return this.dimensions.get(key);
			}
		}

		return null;
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

	public Map<String, String> getDimensions() {
		return this.dimensions;
	}

	public void load() {
		// if has no data source definition,put map data
		if (super.dataSource == null) {
			for (String key : super.tableMaps.keySet()) {
				String value = super.tableMaps.get(key);
				value = ContextMgr.getValue(value);

				this.dimensions.put(key, value);
				this.reverseDimensions.put(value, key);
			}

			return;
		}

		String key = super.tableMaps.firstKey();
		String value = super.tableMaps.get(key);

		Map<String, Object> map = super.dataSource.load(key, value);
		for (Map.Entry<String, Object> entry : map.entrySet()) {
			this.dimensions.put(entry.getKey(), (String)entry.getValue());
		}

		for (String tempKey : this.dimensions.keySet()) {
			this.reverseDimensions.put(this.dimensions.get(tempKey), tempKey);
		}
	}
}
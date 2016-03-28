package com.gsta.bigdata.etl.core.lookup;

import java.util.HashMap;
import java.util.Map;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.core.ContextMgr;
import com.gsta.bigdata.etl.core.ParseException;

/**
 * @author shine
 */
public class LKPTable extends AbstractLKPTable{
	private static final long serialVersionUID = 1950990165957160536L;
	@JsonProperty
	private Map<String, Object> reverseDimensions = new HashMap<String, Object>();

	public LKPTable() {
		super();
	}

	/**
	 * 
	 * @param node
	 * @exception ParseException
	 */
	protected void createChildNode(Element node) throws ParseException {
		super.createChildNode(node);
	}

	/**
	 * 
	 * @param nodeList
	 * @exception ParseException
	 */
	protected void createChildNodeList(NodeList nodeList) throws ParseException {
		super.createChildNodeList(nodeList);
	}

	/**
	 * 
	 * @param element
	 * @exception ParseException
	 */
	protected void initAttrs(Element element) throws ParseException {
		super.initAttrs(element);
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
			if (super.dimensions != null) {
				return super.dimensions.get(key);
			}
		}

		return null;
	}

	public void load() {
		// if has no data source definition,put map data
		if (super.dataSource == null) {
			for (String key : super.tableMaps.keySet()) {
				String value = super.tableMaps.get(key);
				value = ContextMgr.getValue(value);

				super.dimensions.put(key, value);
				this.reverseDimensions.put(value, key);
			}

			return;
		}

		String key = super.tableMaps.firstKey();
		String value = super.tableMaps.get(key);

		super.dimensions = super.dataSource.load(key, value);
		for (String tempKey : super.dimensions.keySet()) {
			this.reverseDimensions.put((String)super.dimensions.get(tempKey), tempKey);
		}
	}
}
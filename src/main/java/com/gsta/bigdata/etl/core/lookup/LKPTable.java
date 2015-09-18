package com.gsta.bigdata.etl.core.lookup;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import javax.xml.xpath.XPathExpressionException;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.gsta.bigdata.etl.core.AbstractETLObject;
import com.gsta.bigdata.etl.core.ChildrenTag;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ContextMgr;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.utils.XmlTools;

/**
 * @author shine
 */
public class LKPTable extends AbstractETLObject implements ILookup {
	@JsonProperty
	private String id;
	@JsonIgnore
	TreeMap<String, String> tableMaps = new TreeMap<String, String>();
	@JsonIgnore
	private AbstractDataSource dataSource = null;
	@JsonProperty
	private Map<String, String> dimensions = new HashMap<String, String>();
	@JsonProperty
	private Map<String, String> reverseDimensions = new HashMap<String, String>();

	public LKPTable() {
		super.tagName = Constants.PATH_LKP_TABLE;

		super.registerChildrenTags(new ChildrenTag(
				Constants.PATH_LKP_DATASOURCE, ChildrenTag.NODE));
		super.registerChildrenTags(new ChildrenTag(
				Constants.PATH_LKP_TABLE_MAP, ChildrenTag.NODE_LIST));
	}

	/**
	 * 
	 * @param node
	 * @exception ParseException
	 */
	protected void createChildNode(Element node) throws ParseException {
		Preconditions.checkNotNull(node, "node is null");

		if (node.getNodeName().equals(Constants.PATH_LKP_DATASOURCE)) {
			try {
				String type = XmlTools.getNodeAttr(node, Constants.ATTR_TYPE);
				if (null != type && !"".equals(type)) {
					this.dataSource = DSFactory.getDataSourceByType(type);
					this.dataSource.init(node);
				}

				String ref = XmlTools.getNodeAttr(node, Constants.ATTR_REF);
				if (null != ref && !"".equals(ref)) {
					this.dataSource = DataSourceMgr.getInstance()
							.getDataSource(ref);
				}
			} catch (XPathExpressionException e) {
				throw new ParseException(e);
			}
		}
	}

	/**
	 * 
	 * @param nodeList
	 * @exception ParseException
	 */
	protected void createChildNodeList(NodeList nodeList) throws ParseException {
		Preconditions.checkNotNull(nodeList, "nodeList is null");

		for (int i = 0; i < nodeList.getLength(); i++) {
			Node element = nodeList.item(i);
			if (element.getNodeType() == Node.ELEMENT_NODE
					&& element.getNodeName().equals(
							Constants.PATH_LKP_TABLE_MAP)) {
				try {
					String key = XmlTools.getNodeAttr((Element) element,
							Constants.ATTR_KEY);
					String value = XmlTools.getNodeAttr((Element) element,
							Constants.ATTR_VALUE);

					this.tableMaps.put(key, value);
				} catch (XPathExpressionException e) {
					throw new ParseException(e);
				}
			}
		}

		if (this.tableMaps.size() <= 0) {
			throw new ParseException("table must hava one or more maps");
		}
	}

	/**
	 * 
	 * @param element
	 * @exception ParseException
	 */
	protected void initAttrs(Element element) throws ParseException {
		this.id = super.getAttr(Constants.ATTR_ID);
		if (null == this.id || "".equals(this.id)) {
			throw new ParseException("table must set id attribute");
		}
	}

	/**
	 * 
	 * @param key
	 */
	public boolean isExist(String key) {
		if (this.dimensions != null) {
			return this.dimensions.containsKey(key);
		}

		return false;
	}

	/**
	 * 
	 * @param key
	 */
	public String getValue(String key) {
		if (this.dimensions != null) {
			return this.dimensions.get(key);
		}

		return null;
	}

	/**
	 * 
	 * @param key
	 * @param isReverse
	 */
	public String getValue(String key, boolean isReverse) {
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

	public void load() {
		// if has no data source definition,put map data
		if (this.dataSource == null) {
			for (String key : this.tableMaps.keySet()) {
				String value = this.tableMaps.get(key);
				value = ContextMgr.getValue(value);

				this.dimensions.put(key, value);
				this.reverseDimensions.put(value, key);
			}

			return;
		}

		String key = this.tableMaps.firstKey();
		String value = this.tableMaps.get(key);

		this.dimensions = this.dataSource.load(key, value);
		for (String tempKey : this.dimensions.keySet()) {
			this.reverseDimensions.put(this.dimensions.get(tempKey), tempKey);
		}
	}

	public String getId() {
		return id;
	}

	public Map<String, String> getDimensions() {
		return dimensions;
	}
}
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
import com.gsta.bigdata.utils.StringUtils;
import com.gsta.bigdata.utils.XmlTools;

public abstract class AbstractLKPTable extends AbstractETLObject implements ILookup{
	private static final long serialVersionUID = 6860744642692654179L;
	@JsonProperty
	protected String id;
	@JsonProperty
	protected String type;
	@JsonIgnore
	protected AbstractDataSource dataSource = null;
	//only direct map has one or more field,general is one key/value
	@JsonIgnore
	protected TreeMap<String, String> tableMaps = new TreeMap<String, String>();
	@JsonProperty
	protected Map<String, Object> dimensions = new HashMap<String, Object>();
	
	public AbstractLKPTable(){
		super.tagName = Constants.PATH_LKP_TABLE;

		super.registerChildrenTags(new ChildrenTag(
				Constants.PATH_LKP_DATASOURCE, ChildrenTag.NODE));
		super.registerChildrenTags(new ChildrenTag(
				Constants.PATH_LKP_TABLE_MAP, ChildrenTag.NODE_LIST));
	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		this.id = super.getAttr(Constants.ATTR_ID);
		if (null == this.id || "".equals(this.id)) {
			throw new ParseException("table must set id attribute");
		}
		
		this.type = super.getAttr(Constants.ATTR_TYPE);
	}

	@Override
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
					this.dataSource = DataSourceMgr.getInstance().getDataSource(ref);
				}
			} catch (XPathExpressionException e) {
				throw new ParseException(e);
			}
		}
	}

	@Override
	protected void createChildNodeList(NodeList nodeList) throws ParseException {
		Preconditions.checkNotNull(nodeList, "nodeList is null");

		for (int i = 0; i < nodeList.getLength(); i++) {
			Node element = nodeList.item(i);
			if (element.getNodeType() == Node.ELEMENT_NODE
					&& element.getNodeName().equals(Constants.PATH_LKP_TABLE_MAP)) {
				try {
					String key = XmlTools.getNodeAttr((Element) element,Constants.ATTR_KEY);
					String value = XmlTools.getNodeAttr((Element) element,Constants.ATTR_VALUE);

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

	public abstract void load();
	
	public String getId() {
		return id;
	}
	
	public Map<String, Object> getDimensions() {
		return dimensions;
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
	
	public static AbstractLKPTable newInstance(Element element)
			throws ParseException {
		Preconditions.checkNotNull(element, "element  is null");

		try {
			String type = XmlTools.getNodeAttr(element, Constants.ATTR_TYPE);
			type = ContextMgr.getValue(type);
			//default type is LKPTable 
			if (type == null || "".equals(type)) {
				return new LKPTable();
			} else {
				String subClassName = StringUtils
						.getPackageName(AbstractLKPTable.class)
						+ StringUtils.upperCaseFirstChar(type);
				return (AbstractLKPTable) Class.forName(subClassName).newInstance();
			}
		} catch (ClassNotFoundException | InstantiationException
				| IllegalAccessException | XPathExpressionException e) {
			throw new ParseException(e);
		}
	}
}

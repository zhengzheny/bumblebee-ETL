package com.gsta.bigdata.etl.core.lookup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.xpath.XPathExpressionException;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.LoadException;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.utils.JDBCUtils;
import com.gsta.bigdata.utils.XmlTools;

/**
 * @author shine
 */
public class MySQLDS extends AbstractDataSource{
	private static final long serialVersionUID = 8103028089308507254L;

	@JsonIgnore
	private String sql;
	
	private final static String ATTR_URL = "url";
	private final static String ATTR_USERNAME = "username";
	private final static String ATTR_PASSWORD = "password";
	private final static String ATTR_SQL = "sql";
	private final static String MYSQL_TYPE = "mysql";

	public MySQLDS() {
		super();
	}

	/**
	 * 
	 * @param node
	 * @exception ParseException
	 */
	protected void createChildNode(Element node) throws ParseException {
		Preconditions.checkNotNull(node, "node is null");
		
		super.createChildNode(node);

		if (node.getNodeType() == Node.ELEMENT_NODE &&
				node.getNodeName().equals(Constants.PATH_LKP_DATASOURCE_FIELDS)) {
			try {
				Element sqlNode = XmlTools.getFirstChildByTagName(node, ATTR_SQL);
				this.sql = XmlTools.getNodeValue(sqlNode);
				if (null == this.sql || this.sql.trim().length() == 0
						|| "".equals(this.sql)) {
					throw new ParseException("datasource must hava sql");
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
	 * @param value
	 * @param key
	 */
	public Map<String, String> load(String key, String value) throws LoadException{
		if ((null == key || "".equals(key))
				|| (null == value || "".equals(value))) {
			throw new LoadException("key or value is null.");
		}

		Map<String, String> properties = super.getProperties();
		String url = "";
		String username = "";
		String password = "";
		for (String strKey : properties.keySet()) {
			if (ATTR_URL.equals(strKey)) {
				url = properties.get(strKey);
			} else if (ATTR_USERNAME.equals(strKey)) {
				username = properties.get(strKey);
			} else if (ATTR_PASSWORD.equals(strKey)) {
				password = properties.get(strKey);
			}
		}

		List<Map<String, Object>> mysqlResult = new ArrayList<Map<String,Object>>();
		try {
			//get jdbc mysql result
			mysqlResult = JDBCUtils.getResult(MYSQL_TYPE, url,
					username, password, sql);
		} catch (Exception e) {
			throw new LoadException(e);
		}

		Map<String, String> retMap = new HashMap<String, String>();
		for (Map<String, Object> map : mysqlResult) {
			String tempKey = "";
			String tempValue = "";
			Set<String> keySet = map.keySet();
			for (String strKey : keySet) {
				if (key.equals(strKey)) {
					tempKey = map.get(key).toString();
				}
				if (value.equals(strKey)) {
					tempValue = map.get(value).toString();
				}
			}
			retMap.put(tempKey, tempValue);
		}
		return retMap;
	}
}
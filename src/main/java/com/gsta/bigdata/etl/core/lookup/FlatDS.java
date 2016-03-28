package com.gsta.bigdata.etl.core.lookup;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.xpath.XPathExpressionException;

import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.LoadException;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.utils.FileUtils;
import com.gsta.bigdata.utils.XmlTools;

/**
 * datasource is local txt file
 * 
 * @author shine
 */
public class FlatDS extends AbstractDataSource {
	private static final long serialVersionUID = 2362258230440696974L;
	@JsonIgnore
	private String delimiter;
	@JsonIgnore
	private List<String> fields = new ArrayList<String>();
	private static final String DEFAULT_DELIMIETER = "\t";
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	public FlatDS(){
		super();
	}
	
	/**
	 * 
	 * @param node
	 * @exception ParseException
	 */
	protected void createChildNode(Element node) throws ParseException {
		Preconditions.checkNotNull(node, "element is null");

		super.createChildNode(node);

		if (node.getNodeName().equals(Constants.PATH_LKP_DATASOURCE_FIELDS)) {
			try {
				this.delimiter = XmlTools.getNodeAttr(node,Constants.ATTR_DELIMITER);
				if (null == this.delimiter || "".equals(this.delimiter)) {
					this.delimiter = DEFAULT_DELIMIETER;
				}
				List<Element> fields = XmlTools.getChildrenByTagName(node,Constants.TAG_FIELD);
				for (Element field : fields) {
					String id = XmlTools.getNodeAttr(field, Constants.ATTR_ID);
					this.fields.add(id);
				}
			} catch (XPathExpressionException e) {
				throw new ParseException(e);
			}
		}

		if (this.fields.size() <= 0) {
			throw new ParseException("datasource must have one or more fields");
		}
	}

	/**
	 * 
	 * @param nodeList
	 * @exception ParseException
	 */
	protected void createChildNodeList(NodeList nodeList) throws ParseException {
		Preconditions.checkNotNull(nodeList, "nodeList is null");

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
	public Map<String, Object> load(String key, String value)
			throws LoadException {
		if ((null == key || "".equals(key))
				|| (null == value || "".equals(value))) {
			throw new LoadException("key or value is null.");
		}

		Map<String, String> properties = super.getProperties();
		List<String> paths = new ArrayList<String>();
		for (String strKey : properties.keySet()) {
			if (Constants.DEFAULT_LKP_DS_PROPERTY_PATH.equals(strKey)) {
				paths.add(properties.get(strKey));
			}
		}

		// get flat result
		Map<String, Object> retMap = new HashMap<String, Object>();
		for (String path : paths) {
			InputStream input = null;
			try {
				input = FileUtils.getInputFile(path);
				String line = null;
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(input, Constants.DEFAULT_ENCODING));
				while ((line = reader.readLine()) != null) {
					String[] lines = line.split(this.delimiter , -1);
					if (lines.length != this.fields.size()) {
						throw new ParseException("datasource field count:"
								+ this.fields.size() + ",but line count:" + lines.length);
					}  
					String tempKey = "";
					String tempValue = "";
					for (int i = 0; i < this.fields.size(); i++) {
						if (key.equals(this.fields.get(i))) {
							tempKey = lines[i];
						}
						if (value.equals(this.fields.get(i))) {
							tempValue = lines[i];
						}
					}
					retMap.put(tempKey, tempValue);
				}

				IOUtils.closeStream(reader);
			} catch (Exception e) {
				throw new LoadException(e);
			} finally {
				if (null != input) {
					try {
						input.close();
					} catch (IOException e) {
						e.printStackTrace();
						logger.error(e.toString());
					}
				}
			}
		}

		return retMap;
	}

	public String getDelimiter() {
		return delimiter;
	}

	public List<String> getFields() {
		return fields;
	}

}
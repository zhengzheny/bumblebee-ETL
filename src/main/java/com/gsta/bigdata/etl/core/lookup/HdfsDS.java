package com.gsta.bigdata.etl.core.lookup;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.LoadException;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.utils.HdfsUtils;

/**
 * datasource is hdfs file
 * 
 * @author shine
 */
public class HdfsDS extends FlatDS {
	private static final long serialVersionUID = 2038963581053413715L;
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	public HdfsDS(){
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
	 * @param value
	 * @param key
	 */
	public Map<String, String> load(String key, String value)
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

		// get hdfs result
		List<String> fields = super.getFields();
		Map<String, String> retMap = new HashMap<String, String>();
		for (String path : paths) {
			InputStream input = null;
			try {
				HdfsUtils hdfs = new HdfsUtils();
				input = hdfs.load(path);
				String line = null;
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(input, Constants.DEFAULT_ENCODING));
				while ((line = reader.readLine()) != null) {
					if (!"".equals(line.trim())) {
						String[] lines = line.split(super.getDelimiter());
						if (lines.length != fields.size()) {
							throw new ParseException("dimension line:" + line
									+ ",datasource field count:"
									+ super.getFields().size()
									+ ",but line count:" + lines.length);
						}
						String tempKey = "";
						String tempValue = "";
						for (int i = 0; i < fields.size(); i++) {
							if (key.equals(fields.get(i))) {
								tempKey = lines[i];
							}
							if (value.equals(fields.get(i))) {
								tempValue = lines[i];
							}
						}
						retMap.put(tempKey, tempValue);
					}
				}

				IOUtils.closeStream(reader);
			} catch (Exception e) {
				e.printStackTrace();
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
}

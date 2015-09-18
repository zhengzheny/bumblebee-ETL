package com.gsta.bigdata.etl.core;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Element;

import com.gsta.bigdata.utils.FileUtils;
import com.gsta.bigdata.utils.XmlTools;

/**
 * context property
 * xml configure file definition:
 <etl>
	<contextProperty location="conf/redis.properties" />
	<contextProperty location="conf/etl.properties" />

	<process id="ex1" outputPath="${outputPath}">

conf/redis.properties file definition:
outputPath=/user/chenc/tt/output

 user can use ${outputPath} instead of "/user/chenc/tt/output"

 * @author tianxq
 *
 */
public class ContextProperty{
	private final static String PATH_CTX_PROPERTY = "/etl/contextProperty";
	private final static String ATTR_LOCATION = "location";
		
	Properties properties = new Properties();
	private static final ContextProperty instance = new ContextProperty();
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	public void init(String configFile){
		InputStream inputStream = null;
		//get xml config directory
		
		String dir = configFile.substring(0,
				configFile.lastIndexOf('/') + 1);
		try {
			inputStream = FileUtils.getInputFile(configFile);
			Document document = XmlTools.loadFromInputStream(inputStream);
			
			NodeList nodeList = XmlTools.getNodeListByPath(document, PATH_CTX_PROPERTY);
			for(int i=0;i<nodeList.getLength();i++){
				Node node = nodeList.item(i);
				if(node.getNodeType() == Node.ELEMENT_NODE){
					String location = XmlTools.getNodeAttr((Element)node, ATTR_LOCATION);
					//properties file must have the same directory with configure file
					location = dir + location;
					this.loadProperties(location);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.toString());
		}finally{
			if(inputStream != null){
				try {
					inputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
					logger.error(e.toString());
				}
			}
		}
	}
	
	private void loadProperties(String file){
		InputStream inputStream = null;
		try {
			inputStream = FileUtils.getInputFile(file);
			this.properties.load(inputStream);
		}catch (Exception e) {
			e.printStackTrace();
			logger.error("file=" + file + " " + e.toString());
		}finally{
			if(inputStream != null){
				try {
					inputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
					logger.error(e.toString());
				}
			}
		}
	}
	
	/**
	 * if key is null,return null.
	 * if key is like ${key},return value from properties according key.
	 * if key isn't like ${key} and has no value from properties file return self.
	 * @param key
	 * @return
	 */
	public String getValue(String key) {
		if (key == null) {
			return null;
		}

		if (key.startsWith(Constants.CONTEXT_PREFIX) && key.endsWith(Constants.CONTEXT_POSTFIX)) {
			String tempkey = key.substring(Constants.CONTEXT_PREFIX.length(),
					key.length() - Constants.CONTEXT_POSTFIX.length());
			
			if (this.properties.containsKey(tempkey)) {
				return this.properties.getProperty(tempkey);
			}
		}
		
		return key;
	}
	
	public static ContextProperty getInstance(){
		return instance;
	}
}

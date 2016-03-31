package com.gsta.bigdata.etl.core.lookup;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.utils.FileUtils;
import com.gsta.bigdata.utils.XmlTools;

/**
 * 
 * @author Shine
 *
 */
public class LookupMgr implements Serializable{
	private static final long serialVersionUID = -1760796135606853073L;
	@JsonProperty
	private DataSourceMgr dataSourceMgr;
	@JsonProperty
	private LKPTableMgr lkpTableMgr;
	private static final LookupMgr instance = new LookupMgr();
	private static final String ATTR_IMPORT = "import";
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	public LookupMgr(){
		
	}
	
	public void init(String configFile){
		InputStream inputStream = null;
		Element lookupNode = null;
		Element datasourcesNode = null;
		try {
			inputStream = FileUtils.getInputFile(configFile);
			
			Document document = XmlTools.loadFromInputStream(inputStream);
			lookupNode = XmlTools.getFirstChildByTagName(
					document.getDocumentElement(), Constants.PATH_LOOKUP);
			if (null != lookupNode && lookupNode.getNodeType() == Node.ELEMENT_NODE) {
				String importPath = XmlTools.getNodeAttr((Element) lookupNode, ATTR_IMPORT);
				if (null != importPath && !"".equals(importPath)) {
					lookupNode =  this.importLookupXml(importPath);
				}
				datasourcesNode = XmlTools.getFirstChildByTagName(lookupNode,Constants.PATH_LKP_DATASOURCES);
			}else{
				return;
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.toString());
		} finally {
			if (null != inputStream) {
				try {
					inputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
					logger.error(e.toString());
				}
			}
		}
				
		this.dataSourceMgr = DataSourceMgr.getInstance();
		this.dataSourceMgr.init(datasourcesNode);
		
		this.lkpTableMgr = LKPTableMgr.getInstance();
		this.lkpTableMgr.init(lookupNode);
		this.lkpTableMgr.loadTables();
	}
	
	public static LookupMgr getInstance(){
		return instance;
	}
	
	public void clone(LookupMgr lookupMgr){
		if(lookupMgr == null){
			return;
		}
		
		this.dataSourceMgr = lookupMgr.getDataSourceMgr();
		this.lkpTableMgr = lookupMgr.getLkpTableMgr();
	}
	
	public DataSourceMgr getDataSourceMgr() {
		return dataSourceMgr;
	}

	private Element importLookupXml(String importPath) {
		InputStream inputStream = null;
		try {
			inputStream = FileUtils.getInputFile(importPath);
			Document document = XmlTools.loadFromInputStream(inputStream);
			Node node = XmlTools.getFirstChildByTagName(
					document.getDocumentElement(), Constants.PATH_LOOKUP);
			if (null != node && node.getNodeType() == Node.ELEMENT_NODE) {
				return (Element) node;
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.toString());
		} finally {
			if (inputStream != null) {
				try {
					inputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
					logger.error(e.toString());
				}
			}
		}

		return null;
	}

	public LKPTableMgr getLkpTableMgr() {
		return this.lkpTableMgr;
	}
}

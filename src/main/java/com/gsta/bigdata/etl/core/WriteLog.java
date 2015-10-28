package com.gsta.bigdata.etl.core;

import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.gsta.bigdata.utils.FileUtils;
import com.gsta.bigdata.utils.XmlTools;

/**
 * Write logs to database
 * 
 * @author Shine
 * 
 */
public class WriteLog extends AbstractETLObject {
	private static final long serialVersionUID = 8256937441978000439L;
	private String property;
	private Logger logger = LoggerFactory.getLogger(getClass());
	private static final WriteLog instance = new WriteLog();

	public WriteLog() {
		super.tagName = Constants.TAG_WRITELOG;
	}

	@Override
	protected void initAttrs(Element element) throws ParseException {

	}

	public void init(String configFile) {
		InputStream inputStream = null;
		Element writeLogNode = null;
		try {
			inputStream = FileUtils.getInputFile(configFile);

			Document document = XmlTools.loadFromInputStream(inputStream);
			writeLogNode = XmlTools.getFirstChildByTagName(
					document.getDocumentElement(), Constants.TAG_WRITELOG);
			if (null != writeLogNode
					&& writeLogNode.getNodeType() == Node.ELEMENT_NODE) {
				this.property = XmlTools.getNodeAttr((Element) writeLogNode,
						Constants.ATTR_PROPERTY);
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
	}

	@Override
	protected void createChildNode(Element node) throws ParseException {
		//has node child node
	}

	@Override
	protected void createChildNodeList(NodeList nodeList) throws ParseException {
		//has no child node list
	}

	public String getProperty() {
		return property;
	}

	public static WriteLog getInstance() {
		return instance;
	}
}

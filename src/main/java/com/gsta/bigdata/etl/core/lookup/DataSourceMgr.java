package com.gsta.bigdata.etl.core.lookup;

import java.util.HashMap;
import java.util.Map;

import javax.xml.xpath.XPathExpressionException;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import com.gsta.bigdata.etl.core.AbstractETLObject;
import com.gsta.bigdata.etl.core.ChildrenTag;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.utils.XmlTools;
/**
 * datasource manager
 * 
 * @author Shine
 *
 */
public class DataSourceMgr extends AbstractETLObject {
	private static final long serialVersionUID = 2450386303954516847L;
	@JsonIgnore
	private Map<String,AbstractDataSource> mapDataSource = new HashMap<String,AbstractDataSource>();
	private static final DataSourceMgr instance = new DataSourceMgr();

	public DataSourceMgr() {
		super.tagName = Constants.PATH_LKP_DATASOURCES;
		
		super.registerChildrenTags(new ChildrenTag(Constants.PATH_LKP_DATASOURCE,
				ChildrenTag.NODE_LIST));
	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		//has no attribute
	}

	@Override
	protected void createChildNode(Element node) throws ParseException {
		//has no child node
	}

	@Override
	protected void createChildNodeList(NodeList nodeList) throws ParseException {
		Preconditions.checkNotNull(nodeList, "nodeList is null");

		for (int i = 0; i < nodeList.getLength(); i++) {
			Node element = nodeList.item(i);
			if (element.getNodeType() == Node.ELEMENT_NODE
					&& element.getNodeName().equals(Constants.PATH_LKP_DATASOURCE)) {
				String type = Constants.DEFAULT_LKP_DS_TYPE;;
				try {
					type = XmlTools.getNodeAttr((Element)element, Constants.ATTR_TYPE);
				} catch (XPathExpressionException e) {
					throw new ParseException(e);
				}
				if (null == type || "".equals(type)) {
					type = Constants.DEFAULT_LKP_DS_TYPE;
				}
				
				AbstractDataSource datasource = DSFactory.getDataSourceByType(type);
				if(datasource == null){
					throw new ParseException("invalid data source type:" + type);
				}
				datasource.init((Element) element);
				
				this.mapDataSource.put(datasource.getId(), datasource);
			}
		}
	}

	public AbstractDataSource getDataSource(String dataSourceId) {
		return this.mapDataSource.get(dataSourceId);
	}
	
	public static DataSourceMgr getInstance(){
		return instance;
	}
}

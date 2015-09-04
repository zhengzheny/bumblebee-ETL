package com.gsta.bigdata.etl.core.lookup;

import java.util.HashMap;
import java.util.Map;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.gsta.bigdata.etl.core.AbstractETLObject;
import com.gsta.bigdata.etl.core.ChildrenTag;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ParseException;

/**
 * @author shine
 */
public class LKPTableMgr extends AbstractETLObject{
	@JsonIgnore
	private Map<String, LKPTable> mapTables = new HashMap<String, LKPTable>();
	private static final LKPTableMgr lkpTableMgr = new LKPTableMgr();

	public LKPTableMgr() {
		super.tagName = Constants.PATH_LOOKUP;
		
		super.registerChildrenTags(new ChildrenTag(Constants.PATH_LKP_DATASOURCES,
				ChildrenTag.NODE));
		super.registerChildrenTags(new ChildrenTag(Constants.PATH_LKP_TABLE,
				ChildrenTag.NODE_LIST));
	}

	/**
	 * 
	 * @param lkpTableMgr
	 */
	public void clone(LKPTableMgr lkpTableMgr) {
		this.mapTables = lkpTableMgr.getMapTables();
	}

	/**
	 * @param id
	 */
	@JsonIgnore
	public LKPTable getTable(String id) {
		return this.mapTables.get(id);
	}

	public void loadTables(){
		for (String tableId : this.mapTables.keySet()) {
			LKPTable table = this.mapTables.get(tableId);
			table.load();
		}
	}

	public static LKPTableMgr getInstance() {
		return lkpTableMgr;
	}

	public Map<String, LKPTable> getMapTables() {
		return mapTables;
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
					&& element.getNodeName().equals(Constants.PATH_LKP_TABLE)) {
				LKPTable lkpTable = new LKPTable();
				lkpTable.init((Element) element);
				
				this.mapTables.put(lkpTable.getId(), lkpTable);
			}
		}

		if (this.mapTables.size() <= 0) {
			throw new ParseException(
					"datasource must have one or more tables");
		}
	}
}
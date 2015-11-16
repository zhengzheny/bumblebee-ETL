package com.gsta.bigdata.etl.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.gsta.bigdata.etl.core.function.dpi.DpiRule;

/**
 * 
 * @author tianxq
 *
 */
public class GeneralRuleMgr extends AbstractETLObject{
	private static final long serialVersionUID = 2701432344918742592L;
	
	private static final GeneralRuleMgr instance = new GeneralRuleMgr();
	
	@JsonProperty
	private Map<String,IRuleMgr> ruleMgrs = new HashMap<String,IRuleMgr>();
	private List<DpiRule> dpiRules = new ArrayList<DpiRule>();
	
	private static final String RULE_MGR_PACKAGE_NAME = "com.gsta.bigdata.etl.core.function.dpi";
	
	public GeneralRuleMgr(){
		super.tagName = Constants.PATH_DPI_FUNCTIONRULES;
		
		super.registerChildrenTags(new ChildrenTag(Constants.PATH_DPI_RULE,
				ChildrenTag.NODE_LIST));
	}
	
	@Override
	protected void initAttrs(Element element) throws ParseException {
		
	}

	@Override
	protected void createChildNode(Element node) throws ParseException {
		
	}
	
	

	@Override
	protected void createChildNodeList(NodeList nodeList) throws ParseException {
		Preconditions.checkNotNull(nodeList, "nodeList is null");

		for (int i = 0; i < nodeList.getLength(); i++) {
			Node element = nodeList.item(i);
			if (element.getNodeType() == Node.ELEMENT_NODE
					&& element.getNodeName().equals(Constants.PATH_DPI_RULE)) {
				DpiRule dpiRule = new DpiRule();
				dpiRule.init((Element)element);
				this.dpiRules.add(dpiRule);
			}
		}
		
	}
	
	
	@Override
	public void init(Element element) throws ParseException {
		super.init(element);
		
		if(this.dpiRules != null || this.dpiRules.size() > 0){
			for(DpiRule dpiRule : this.dpiRules){
				String ruleId = dpiRule.getId();
				try {
					IRuleMgr ruleMgr = (IRuleMgr)Class.forName(RULE_MGR_PACKAGE_NAME + "." + ruleId).newInstance();
					register(ruleId, ruleMgr);
				} catch (InstantiationException | IllegalAccessException
						| ClassNotFoundException e) {
					throw new ParseException("can't find rule manager:" + ruleId);
				}
			}
		}
	}

	public void register(String ruleId,IRuleMgr ruleMgr){
		this.ruleMgrs.put(ruleId,ruleMgr);
	}
	
	public void clone(GeneralRuleMgr ruleStatisMgr){
		this.ruleMgrs.clear();
		this.ruleMgrs.putAll(ruleStatisMgr.getRuleMgrs());
	}

	public static GeneralRuleMgr getInstance() {
		return instance;
	}

	public Map<String,IRuleMgr> getRuleMgrs() {
		return ruleMgrs;
	}
	
	
	
	public IRuleMgr getRuleMgrById(String ruleId){
		return ruleMgrs.get(ruleId);
	}
	
	public DpiRule getDpiRuleById(String ruleId){
		if(StringUtils.isBlank(ruleId)){
			return null;
		}
		
		for(DpiRule dpiRule : this.dpiRules){
			if(dpiRule.getId().equals(ruleId)){
				return dpiRule;
			}
		}
		
		return null;
	}

}

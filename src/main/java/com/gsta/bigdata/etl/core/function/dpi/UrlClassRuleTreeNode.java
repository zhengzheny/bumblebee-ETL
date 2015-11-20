package com.gsta.bigdata.etl.core.function.dpi;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;

public class UrlClassRuleTreeNode implements Serializable{
	private static final long serialVersionUID = 908591203467127422L;
	
	@JsonProperty
	private String tag = null; 
	@JsonProperty
    private List<UrlClassRule> rules;
	@JsonProperty
    private long ruleCounter = 0; 
	@JsonProperty
    private int deep = 0; 
	@JsonBackReference
    private UrlClassRuleTreeNode parent = null; 
	@JsonManagedReference
    private UrlClassRuleTreeNode sibling = null; 
	@JsonManagedReference
    private UrlClassRuleTreeNode firstChild = null; 
	
	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

	public List<UrlClassRule> getRules() {
		return rules;
	}

	public void setRules(List<UrlClassRule> rules) {
		this.rules = rules;
	}

	public long getRuleCounter() {
		return ruleCounter;
	}

	public void setRuleCounter(long ruleCounter) {
		this.ruleCounter = ruleCounter;
	}

	public int getDeep() {
		return deep;
	}

	public void setDeep(int deep) {
		this.deep = deep;
	}

	public UrlClassRuleTreeNode getParent() {
		return parent;
	}

	public void setParent(UrlClassRuleTreeNode parent) {
		this.parent = parent;
	}

	public UrlClassRuleTreeNode getSibling() {
		return sibling;
	}

	public void setSibling(UrlClassRuleTreeNode sibling) {
		this.sibling = sibling;
	}

	public UrlClassRuleTreeNode getFirstChild() {
		return firstChild;
	}

	public void setFirstChild(UrlClassRuleTreeNode firstChild) {
		this.firstChild = firstChild;
	}

    public UrlClassRuleTreeNode()
    {
    }
    
    public void incrementRuleCounter()
    {
    	ruleCounter++;
    }
}

package com.gsta.bigdata.etl.core;

import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 
 * @author tianxq
 *
 */
public class RuleStatisMgr {
	private static final RuleStatisMgr instance = new RuleStatisMgr();
	@JsonProperty
	private Set<IRuleMgr> ruleMgrs;

	public RuleStatisMgr() {
		ruleMgrs = new HashSet<IRuleMgr>();
	}
	
	public void register(IRuleMgr ruleMgr){
		this.ruleMgrs.add(ruleMgr);
	}
	
	public void clone(RuleStatisMgr ruleStatisMgr){
		this.ruleMgrs = ruleStatisMgr.getRuleMgrs();
	}

	public static RuleStatisMgr getInstance() {
		return instance;
	}

	public Set<IRuleMgr> getRuleMgrs() {
		return ruleMgrs;
	}
}

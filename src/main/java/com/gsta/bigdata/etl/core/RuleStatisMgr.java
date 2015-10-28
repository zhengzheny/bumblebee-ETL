package com.gsta.bigdata.etl.core;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 
 * @author tianxq
 *
 */
public class RuleStatisMgr implements Serializable{
	private static final long serialVersionUID = 2701432344918742592L;
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

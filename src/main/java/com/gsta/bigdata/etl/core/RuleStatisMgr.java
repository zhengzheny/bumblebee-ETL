package com.gsta.bigdata.etl.core;

import java.util.HashSet;
import java.util.Set;

/**
 * 
 * @author tianxq
 *
 */
public class RuleStatisMgr {
	private static final RuleStatisMgr instance = new RuleStatisMgr();
	private Set<IRuleMgr> ruleMgrs;

	public RuleStatisMgr() {
		ruleMgrs = new HashSet<IRuleMgr>();
	}
	
	public void register(IRuleMgr ruleMgr){
		this.ruleMgrs.add(ruleMgr);
	}

	public static RuleStatisMgr getInstance() {
		return instance;
	}

	public Set<IRuleMgr> getRuleMgrs() {
		return ruleMgrs;
	}
}

package com.gsta.bigdata.etl.core;

import java.util.Map;

/**
 * rule manager,get statistical information from rule manager
 * and print statistical information when mapper cleanup.
 * @author tianxq
 *
 */
public interface IRuleMgr {
	/**
	 * return rule manager statistical information
	 * @return
	 */
	public String getStatInfo();
	
	/**
	 * return matched rule statistical information
	 * @return key-rule info;value-rule matched count
	 */
	public Map<String, Long> getRuleMatchedStats();
	
	/**
	 * write statistical information file prefix
	 * @return
	 */
	public String getId();
	
	public String getStatisFileDir();
}

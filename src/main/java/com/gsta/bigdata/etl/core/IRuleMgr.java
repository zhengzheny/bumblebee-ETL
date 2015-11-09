package com.gsta.bigdata.etl.core;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.gsta.bigdata.etl.core.function.dpi.DpiRule;
import com.gsta.bigdata.etl.core.function.dpi.SearchWordsRuleManager;
import com.gsta.bigdata.etl.core.function.dpi.UrlClassRuleManager;
import com.gsta.bigdata.etl.core.function.dpi.UserAgentRuleManager;

/**
 * rule manager,get statistical information from rule manager and print
 * statistical information when mapper cleanup.
 * 
 * @author tianxq
 *
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
@JsonSubTypes({@JsonSubTypes.Type(value = SearchWordsRuleManager.class, name = "SearchWordsRuleManager"),
			   @JsonSubTypes.Type(value = UrlClassRuleManager.class, name = "UrlClassRuleManager"),
		       @JsonSubTypes.Type(value = UserAgentRuleManager.class, name = "UserAgentRuleManager") })
public interface IRuleMgr {
	/**
	 * return rule manager statistical information
	 * 
	 * @return
	 */
	public String getStatInfo();

	/**
	 * return matched rule statistical information
	 * 
	 * @return key-rule info;value-rule matched count
	 */
	public Map<String, Long> getRuleMatchedStats();

	/**
	 * write statistical information file prefix
	 * 
	 * @return
	 */
	public String getId();

	public String getFilePath();

	public String getStatisFileDir();

	public DpiRule getDpiRule();
}

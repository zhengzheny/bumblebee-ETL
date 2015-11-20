package com.gsta.bigdata.etl.core.function.dpi;

import java.io.InputStream;
import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.core.GeneralRuleMgr;
import com.gsta.bigdata.etl.core.IRuleMgr;
import com.gsta.bigdata.utils.FileUtils;

/**
 * @author shine
 *
 */
public class SearchWordsRuleManager implements IRuleMgr,Serializable{
	private static final long serialVersionUID = -3385602165404882931L;

	@JsonProperty
	private SearchWordsRule[] allRules;
	
	private Properties properties = new Properties();
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	private DpiRule dpiRule;
	
	private static final String RULE_DELIMITER = "|";
	private static final String PARAM_URL_KEY_PREFIX = "SearchWordsRule@";
	
	public SearchWordsRuleManager() {
		
	}

	public void init()
	{
		this.dpiRule = GeneralRuleMgr.getInstance().getDpiRuleById(getClass().getSimpleName());
		String filePath = this.dpiRule.getFilePath();
		Map<Integer,String> fmtedRuleItems = formatRules(filePath,PARAM_URL_KEY_PREFIX);
		if (fmtedRuleItems == null)
		{
			return;
		}
		allRules = sortAndCreateRuleObjects(fmtedRuleItems);
	}
	
	private Map<Integer, String> formatRules(String path,String prefix) {
		Map<Integer,String> retMap = new HashMap<Integer,String>();
		
		try {
			InputStream input = FileUtils.getInputFile(path);
			properties.load(input);
			Set<Entry<Object, Object>> entrySet = properties.entrySet();
			for(Entry<Object,Object> entry: entrySet){
				String ruleName = entry.getKey().toString();
				int index = ruleName.indexOf(prefix);
				if(index != -1){
					String ruleContent = entry.getValue().toString();
					String ruleId = ruleName.substring(index + prefix.length());
					retMap.put(Integer.parseInt(ruleId), ruleContent);
				}
			}
		} catch (Exception e) {
			logger.error("can't find the file path:" + path);
			return null;
		}
		
		return retMap;
	}
	
	
	/**
	 * @return
	 */
	public SearchWordsRule[] getRules()
	{
		return this.allRules;
	}
	
	
	/**
	 * @param map
	 * @return
	 */
	private SearchWordsRule[] sortAndCreateRuleObjects(Map<Integer,String> map)
	{
		if (map == null)
		{
			return null;
		}
		Map<Integer, String> treeMap = new TreeMap<Integer,String>(new Comparator<Integer>(){ 
			   public int compare(Integer obj1, Integer obj2){ 
			    return obj1.compareTo(obj2); 
			   } 
			  }); 
		
		treeMap.putAll(map);
		SearchWordsRule[] list = new SearchWordsRule[treeMap.size()];
		Set<Integer> keySet = treeMap.keySet(); 
		Iterator<Integer> iter = keySet.iterator();
		int i = 0;
		while(iter.hasNext()){ 
			Integer key = iter.next(); 
			Object value = treeMap.get(key);
			SearchWordsRule rule = parseRule(key.toString(), (String)value);
			list[i] = rule;
			i++;
		}
		return list;
	}

	/**
	 * @param map
	 * @return
	 */
	public SearchWordsRule[] sortAndCreateServiceRuleMap(Map<Integer,String> map)
	{
		if (map == null)
		{
			return null;
		}
		Map<Integer, String> treeMap = new TreeMap<Integer,String>(new Comparator<Integer>(){ 
			   public int compare(Integer obj1, Integer obj2){ 
			    return obj1.compareTo(obj2); 
			   } 
			  }); 
		
		treeMap.putAll(map);
		SearchWordsRule[] list = new SearchWordsRule[treeMap.size()];
		Set<Integer> keySet = treeMap.keySet(); 
		Iterator<Integer> iter = keySet.iterator();
		int i = 0;
		while(iter.hasNext()){ 
			Integer key = iter.next(); 
			Object value = treeMap.get(key);
			SearchWordsRule rule = parseRule(key.toString(), (String)value);
			list[i] = rule;
			i++;
		}
		return list;
	}
	
	
	/**
	 * @param key
	 * @param value
	 * @return
	 */
	private SearchWordsRule parseRule(String ruleID, String ruleValue)
	{
		if (ruleValue == null)
		{
			return null;
		}
		
		// 1. split the group index
		int delimiterIndex = ruleValue.indexOf(RULE_DELIMITER); 
		if (delimiterIndex == -1)
		{
			return null;
		}
		String groupIndexStr = ruleValue.substring(0, delimiterIndex);
		int groupIndex = -1;
		try {
			groupIndex = Integer.parseInt(groupIndexStr);
		} catch (Exception e)
		{
		}

		// 2. split the charset
		String urlRule = ruleValue.substring(delimiterIndex + RULE_DELIMITER.length());
		delimiterIndex = urlRule.indexOf(RULE_DELIMITER); 
		if (delimiterIndex == -1)
		{
			return null;
		}
		String charset = urlRule.substring(0, delimiterIndex);
		
		// 3. split the decoder
		urlRule = urlRule.substring(delimiterIndex + RULE_DELIMITER.length());
		delimiterIndex = urlRule.indexOf(RULE_DELIMITER); 
		if (delimiterIndex == -1)
		{
			return null;
		}
		String decoder = urlRule.substring(0, delimiterIndex);
		String regular = urlRule.substring(delimiterIndex + RULE_DELIMITER.length());
		
		SearchWordsRule rule = new SearchWordsRule();
		rule.setId(ruleID);
		rule.setRegular(regular);
		rule.setCharset(charset);
		rule.setGroupIndex(groupIndex);
		rule.setDecoder(decoder);
		return rule;
	}
	
	@Override
	@JsonIgnore
	public DpiRule getDpiRule() {
		return dpiRule;
	}

	@Override
	@JsonIgnore
	public String getStatInfo() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	@JsonIgnore
	public Map<String, Long> getRuleMatchedStats() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	@JsonIgnore
	public String getId() {
		return this.dpiRule.getId();
	}

	@Override
	@JsonIgnore
	public String getStatisFileDir() {
		return this.dpiRule.getStatisFileDir();
	}

	@Override
	@JsonIgnore
	public String getFilePath() {
		return this.dpiRule.getFilePath();
	}
}

package com.gsta.bigdata.etl.core.function.dpi;

import java.io.InputStream;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.core.IRuleMgr;
import com.gsta.bigdata.utils.FileUtils;

/**
 * @author xiangy
 *
 */
public class UserAgentRuleManager implements IRuleMgr{
	public static final int RULE_MATCH_INDEXES_COUNT = 8;
	public static final String RULE_DELIMITER = "\\|\\|";
	public static final String RULE_INDEX_DELIMITER = ",";
	public static final String SERVICE_RULE_INDEX_DELIMITER = ",";
	
	private static final String PARAM_RULE_KEY_PREFIX = "rule@";
	//private static final String PARAM_SERVICE_KEY_PREFIX = "service.key.prefix";
	private static final String PARAM_INPUT_USERAGENT_KEYWORDS = "input.useragent.keywords";

	@JsonProperty
	private Map<String, UseragentParserRule[]> serviceRuleMap = new HashMap<String, UseragentParserRule[]>(); 
	@JsonProperty
	private UseragentParserRule[] allUeragentRules;
	@JsonProperty
	private static Pattern useragentKeywordPattern;
	
	private Properties properties = new Properties();
	@JsonIgnore
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	public Pattern getUseragentKeywordPattern() {
		return useragentKeywordPattern;
	}

	public static void setUseragentKeywordPattern(Pattern useragentKeywordPattern) {
		UserAgentRuleManager.useragentKeywordPattern = useragentKeywordPattern;
	}

	
	
	public UserAgentRuleManager() {
		
	}

	public void init(String path)
	{
		initAllRuleList(path);

		//initServiceRuleAfterAllRuleListLoaded(path);

		initKeywordsRule(path);
	}
	
	/**
	 * init keyword rule
	 */
	private void initKeywordsRule(String path){
		try {
			InputStream input = FileUtils.getInputFile(path);
			properties.load(input);
			String keywords = properties.getProperty(PARAM_INPUT_USERAGENT_KEYWORDS);
			if (keywords != null && keywords.trim().length() > 0) {
				useragentKeywordPattern = Pattern.compile(keywords.trim(), Pattern.CASE_INSENSITIVE);
			}
		} catch (Exception e) {
			logger.error("can't find the file path:" + path);
			return;
		}
		
	}

	private void initAllRuleList(String path)
	{
		Map<Integer,String> fmtedRuleItems = formatRules(path,PARAM_RULE_KEY_PREFIX);
		if (fmtedRuleItems == null)
		{
			return;
		}
		allUeragentRules = sortAndCreateRuleObjects(fmtedRuleItems);
	}
	

/*	private void initServiceRuleAfterAllRuleListLoaded(String path)
	{
		Map<Integer,String> fmtedServiceItems = formatRules(path, PARAM_SERVICE_KEY_PREFIX);
		if (fmtedServiceItems == null)
		{
			return;
		}
		
		Set<Integer> keySet = fmtedServiceItems.keySet(); 
		Iterator<Integer> iter = keySet.iterator(); 
		while(iter.hasNext()){ 
			Integer key = iter.next();
			if (key == null)
			{
				continue;
			}
			String ruleIDs = (String)fmtedServiceItems.get(key);
			if (ruleIDs == null)
			{
				continue;
			}
			int indexPrefix = key.toString().indexOf(PARAM_SERVICE_KEY_PREFIX);
			if (indexPrefix == -1)
			{
				continue;
			}
			String serviceID = key.toString().substring(indexPrefix + PARAM_SERVICE_KEY_PREFIX.length()).trim();
			String[] ruleIDItems = ruleIDs.split(SERVICE_RULE_INDEX_DELIMITER);
			if (ruleIDItems == null)
			{
				continue;
			}
			int ruleCount=ruleIDItems.length;
			UseragentParserRule[] serviceRules = new UseragentParserRule[ruleCount]; 
			for (int i = 0; i < ruleCount; i++)
			{
				if (ruleIDItems[i] == null)
				{
					continue;
				}
				ruleIDItems[i] = ruleIDItems[i].trim();
				UseragentParserRule rule = getRuleFromAllRuleListByID(ruleIDItems[i]);
				if (rule == null)
				{
					continue;
				}
				serviceRules[i] = rule;
			}
			this.serviceRuleMap.put(serviceID, serviceRules);
		}
	}*/
	
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
					String ruleContent = entry.getValue().toString().trim();
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
	 * @param ruleID
	 * @return
	 */
	/*private UseragentParserRule getRuleFromAllRuleListByID(String ruleID)
	{
		if (allUeragentRules == null)
		{
			return null;
		}
		
		int ruleCount=this.allUeragentRules.length;
		for (int i = 0; i < ruleCount; i++)
		{
			UseragentParserRule rule = allUeragentRules[i];
			if (rule == null)
			{
				continue;
			}
			if (ruleID.equalsIgnoreCase(rule.getId()))
			{
				return rule;
			}
		}
		return null;
	}*/
	
	/**
	 * @return
	 */
	public UseragentParserRule[] getRules()
	{
		return this.allUeragentRules;
	}
	
	/**
	 * @param serviceType
	 * @return
	 */
	public UseragentParserRule[] getRules(String serviceType)
	{
		if (serviceType == null) return null;
		
		if (serviceRuleMap.containsKey(serviceType))
		{
			return serviceRuleMap.get(serviceType);
		}
		
		return null;
	}
	
	
	/**
	 * @param map
	 * @return
	 */
	public UseragentParserRule[] sortAndCreateRuleObjects(Map<Integer,String> map)
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
		UseragentParserRule[] list = new UseragentParserRule[treeMap.size()];
		Set<Integer> keySet = treeMap.keySet(); 
		Iterator<Integer> iter = keySet.iterator();
		int i = 0;
		while(iter.hasNext()){ 
			Integer key = iter.next(); 
			Object value = treeMap.get(key);
			UseragentParserRule rule = createRule(key.toString(), (String)value);
			list[i] = rule;
			i++;
		}
		return list;
	}

	/**
	 * @param map
	 * @return
	 */
	public UseragentParserRule[] sortAndCreateServiceRuleMap(Map<Integer,String> map)
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
		UseragentParserRule[] list = new UseragentParserRule[treeMap.size()];
		Set<Integer> keySet = treeMap.keySet(); 
		Iterator<Integer> iter = keySet.iterator();
		int i = 0;
		while(iter.hasNext()){ 
			Integer key = iter.next(); 
			Object value = treeMap.get(key);
			UseragentParserRule rule = createRule(key.toString(), (String)value);
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
	private UseragentParserRule createRule(String ruleID, String ruleValue)
	{
		if (ruleValue == null)
		{
			return null;
		}
		
		String[] items = ruleValue.split(RULE_DELIMITER); 
		if (items == null || items.length != 2 || items[0] == null || items[1] == null)
		{
			return null;
		}
		
		String[] indexItems = items[1].trim().split(RULE_INDEX_DELIMITER);
		if (indexItems == null || indexItems.length != RULE_MATCH_INDEXES_COUNT)
		{
			return null;
		}
		int indexCount = indexItems.length;
		int[] indexes = new int[indexCount];
		for (int i = 0; i < indexCount; i++)
		{
			indexes[i] = Integer.parseInt(indexItems[i]);
			if (indexes[i] > 8)
			{
				return null;
			}
		}
			
		UseragentParserRule rule = new UseragentParserRule();
		rule.setId(ruleID);
		rule.setIndexes(indexes);
		rule.setRegular(items[0]);
		
		return rule;
	}
	
	@Override
	@JsonIgnore
	public String getStatInfo() {
		return null;
	}

	@Override
	@JsonIgnore
	public Map<String, Long> getRuleMatchedStats() {
		return null;
	}
	
	@Override
	@JsonIgnore
	public String getId() {
		return null;
	}

	@Override
	@JsonIgnore
	public String getStatisFileDir() {
		return null;
	}
	
}

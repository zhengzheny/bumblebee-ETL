package com.gsta.bigdata.etl.core.function.dpi;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonProperty;

public class UrlClassRule {
	public static final String RULE_WILDCARD_SYMBOLS = "\\*";
	public static final String RULE_KEYWORDS_DELIMITER = "\\|";

	public static final String RULE_MATCH_FULL = "0";
	public static final String RULE_MATCH_LEFT = "1";
	public static final String RULE_MATCH_RIGHT = "2";
	public static final String RULE_MATCH_ANY = "3";
	public static final String RULE_MATCH_LEFT_RIGHT = "4";
	public static final String RULE_MATCH_REGULAR = "5";

	public static final int RULE_FIELDS_COUNT = 9;
	public static final int RULE_FIELD_RULE_INDEX = 0;
	public static final int RULE_FIELD_MATCH_TYPE_INDEX = 1;
	public static final int RULE_FIELD_MATCH_NAME_INDEX = 2;
	public static final int RULE_FIELD_CLASS_ID_INDEX = 3;
	public static final int RULE_FIELD_CLASS_LEVEL_INDEX = 4;
	public static final int RULE_FIELD_CLASS_NAME_INDEX = 5;
	public static final int RULE_FIELD_KEYWORDS_INDEX = 6;
	public static final int RULE_FIELD_ID_INDEX = 7;
	public static final int RULE_FIELD_PRIORITY_INDEX = 8;
	
	public static final String RULE_LEVEL_SPAM = "-1";
	public static final String RULE_LEVEL_0 = "0";
	public static final String RULE_LEVEL_1 = "1";
	public static final String RULE_LEVEL_2 = "2";
	public static final String RULE_LEVEL_3 = "3";
	public static final String RULE_LEVEL_4 = "4";
	public static final String RULE_LEVEL_5 = "5";
	public static final String RULE_LEVEL_6 = "6";
	public static final String DEFAULT_URL_CLASS_ID = "-1";
	
	@JsonProperty
	protected Map<String/*urlRule*/, String/*dpidata*/> matchErrorRules = new HashMap<String/*urlRule*/, String/*dpidata*/>(10000);

	@JsonProperty
	protected String ruleId;
	@JsonProperty
	protected String source;
	@JsonProperty
	protected String rule;
	@JsonProperty
	protected String[] keywords;
	@JsonProperty
	protected Pattern rulePattern; 
	@JsonProperty
	protected String typeId;
	@JsonProperty
	protected String typeName;
	@JsonProperty
	protected String classId = DEFAULT_URL_CLASS_ID;
	@JsonProperty
	protected String className;
	@JsonProperty
	protected String level;
	@JsonProperty
	protected int priority = 0; 
	
	public UrlClassRule() {
	}

	public String getRuleId() {
		return ruleId;
	}

	public void setRuleId(String ruleId) {
		this.ruleId = ruleId;
	}

	public String[] getKeywords() {
		return keywords;
	}

	public void setKeywords(String[] keywords) {
		this.keywords = keywords;
	}

	public String getTypeId() {
		return typeId;
	}

	public void setTypeId(String typeId) {
		this.typeId = typeId;
	}

	public String getTypeName() {
		return typeName;
	}

	public void setTypeName(String typeName) {
		this.typeName = typeName;
	}

	public int getPriority() {
		return priority;
	}

	public void setPriority(int priority) {
		this.priority = priority;
	}

	public String getLevel() {
		return level;
	}

	public void setLevel(String level) {
		this.level = level;
	}

	public String getRule() {
		return rule;
	}

	public void setRule(String rule) {
		this.rule = rule;
	}

	public Pattern getRulePattern() {
		return rulePattern;
	}

	public void setRulePattern(Pattern rulePattern) {
		this.rulePattern = rulePattern;
	}

	public String getClassId() {
		return classId;
	}

	public void setClassId(String classId) {
		this.classId = classId;
	}

	public String getClassName() {
		return className;
	}

	public void setClassName(String className) {
		this.className = className;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public boolean isMatch(String url) {
		if (url == null || rule == null)
			return false;
		if (matchErrorRules.containsKey(getRule()))
		{
			return false;
		}
		return true;
	}
	
	/**
	 * 检查URL是否含有所有关键字
	 * @param url
	 * @param keywords
	 * @param bOrder: 是否需要按照关键字顺序匹配
	 * @return
	 */
	protected boolean hasKeywords(String url, boolean bAll, boolean bOrder)
	{
		if (url == null || keywords == null || keywords.length == 0)
		{
			return false;
		}
		
		int keyCount = keywords.length;
		if (keyCount == 1 && keywords[0] != null )//包含一个关键词
		{
			return url.indexOf(keywords[0]) != -1;
		}
		
		int searchStartIndex = 0;
		for (int k = 0; k < keyCount; k++)
		{
			if (keywords[k] == null)
			{
				continue;
			}
			String key = keywords[k].trim();
			if (key.equals(""))
			{
				continue;
			}

			if (k == 0 && !key.equals(""))//以该关键字打头
			{
				if (!url.startsWith(key) && bAll) return false;
			}
			else if (k == keyCount - 1 && !key.equals(""))//以该关键字结尾
			{
				if (!url.endsWith(key) && bAll) return false;
			}
			else
			{
				if (bOrder)
					searchStartIndex = url.indexOf(key, searchStartIndex);
				else
					searchStartIndex = url.indexOf(key);
				if (searchStartIndex == -1 && bAll)
				{
					return false;
				}
			}
			searchStartIndex = searchStartIndex + key.length();
		}
		
		return true;
	}

}

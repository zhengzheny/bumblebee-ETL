package com.gsta.bigdata.etl.core.function.dpi;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.core.GeneralRuleMgr;
import com.gsta.bigdata.etl.core.IRuleMgr;
import com.gsta.bigdata.utils.FileUtils;

public class UrlClassRuleManager implements IRuleMgr,Serializable {
	private static final long serialVersionUID = -4794161669516529422L;

	@JsonIgnore
	public final Logger logger = LoggerFactory.getLogger(getClass());

	private final int RULE_INDEX_KEY_LENGTH = 2;
	private final String validChars = "abcdefghijklmnopqrstuvwxyz0123456789./?=&_%\\|^$";
	private static final Pattern validRuleCheckPatn = Pattern.compile(
			"^[0-9a-zA-Z]+$", Pattern.CASE_INSENSITIVE);

	private String ruleDelimiter = "\t";
	private String ruleCharset = "UTF-8";

	private DpiRule dpiRule;
	private RuleCounter ruleCounter;
	
	private Map<String, String> ruleCheckMap = new HashMap<String, String>(
			10000);
	private Map<String, List<UrlClassRule>> ruleListMap = new HashMap<String, List<UrlClassRule>>();
	@JsonProperty
	private Map<String, UrlClassRuleTreeNode> rulesTreeMap = new HashMap<String, UrlClassRuleTreeNode>();

	public UrlClassRuleManager() {

	}

	/**
	 * init data
	 */
	public void init() throws IOException {
		printJVMInfo("debug:load before--");
		long loadStartTime = System.currentTimeMillis();
		if (ruleCheckMap != null) {
			ruleCheckMap.clear();
		} else {
			ruleCheckMap = new HashMap<String, String>();
		}
		// 1. load rules
		this.dpiRule = GeneralRuleMgr.getInstance().getDpiRuleById(
				getClass().getSimpleName());
		String filePath = this.dpiRule.getFilePath();
		loadUrlClassRules(filePath);
		if (ruleCheckMap != null) {
			ruleCheckMap.clear();
		}

		// 2. create rule trees
		this.initRuleTree(UrlClassRule.RULE_LEVEL_SPAM);
		this.initRuleTree(UrlClassRule.RULE_LEVEL_6);
		this.initRuleTree(UrlClassRule.RULE_LEVEL_5);
		this.initRuleTree(UrlClassRule.RULE_LEVEL_4);
		this.initRuleTree(UrlClassRule.RULE_LEVEL_3);
		this.initRuleTree(UrlClassRule.RULE_LEVEL_2);
		this.initRuleTree(UrlClassRule.RULE_LEVEL_1);
		this.initRuleTree(UrlClassRule.RULE_LEVEL_0);

		if (ruleListMap != null) {
			ruleListMap.clear();
		}

		long loadEndTime = System.currentTimeMillis();
		logger.info("loading the rules of URL class: "
				+ (loadEndTime - loadStartTime) / 1000 + " seconds "
				+ " rule count=" + this.ruleCheckMap.size());
		printJVMInfo("debug:load after--");
	}

	private void loadUrlClassRules(String path) {
		InputStream inputStream = null;
		BufferedReader reader = null;
		String line = null;
		try {
			inputStream = FileUtils.getInputFile(path);
			reader = new BufferedReader(new InputStreamReader(inputStream,
					ruleCharset));
			ruleCheckMap.clear();
			while ((line = reader.readLine()) != null) {
				if (line.trim().length() == 0) {
					continue;
				}
				if (line.startsWith("#")) {
					continue;
				}
				UrlClassRule rule = parseUrlClassRule(line);
				if (rule == null || rule.getRule() == null
						|| rule.getClassId() == null
						|| rule.getTypeId() == null) {
					logger.warn("Invalid rule: " + line);
				} else if (!ruleCheckMap.containsKey(rule.getRule())) {
					cacheUrlClassRule(rule);
				}
			}
			ruleCheckMap.clear();
		} catch (Exception e) {
			logger.error("read rule error", e.getMessage());
			return;
		} finally {
			try {
				if (reader != null) {
					reader.close();
				}
				if (inputStream != null) {
					inputStream.close();
				}
			} catch (Exception e2) {

			} finally {
				reader = null;
				inputStream = null;
			}
		}

	}

	public void clean() {
		if (ruleCheckMap != null)
			ruleCheckMap.clear();
		ruleCheckMap = null;

		if (ruleListMap != null)
			ruleListMap.clear();
		ruleListMap = null;

		if (rulesTreeMap != null)
			rulesTreeMap.clear();
		rulesTreeMap = null;
	}

	/**
	 * 
	 * @param level
	 * @return
	 */
	@JsonProperty
	public UrlClassRuleTreeNode getRuleTree(String level) {
		if (rulesTreeMap == null)
			return null;
		return (UrlClassRuleTreeNode) rulesTreeMap.get(level);
	}

	private void initRuleTree(String level) {
		UrlClassRuleTreeNode treeRoot = new UrlClassRuleTreeNode();
		List<UrlClassRule> rulesOfTree = (ArrayList<UrlClassRule>) ruleListMap
				.get(level);
		treeRoot.setRules(rulesOfTree);
		treeRoot.setTag("");
		createUrlClassRulesTree(treeRoot);
		rulesTreeMap.put(level, treeRoot);
		if (rulesOfTree != null)
			logger.info("--level " + level + "'s rules count="
					+ rulesOfTree.size());
	}

	/**
	 * 
	 * @param tag
	 * @return
	 */
	private List<String> extendTag(String matchIndexTag) {
		if (matchIndexTag == null) {
			return null;
		}
		int charCnt = validChars.length();
		List<String> extTags = new ArrayList<String>(charCnt * 2);
		ruleCheckMap.clear();
		for (int i = 0; i < charCnt; i++) {
			String newTag = validChars.charAt(i) + matchIndexTag;
			if (!ruleCheckMap.containsKey(newTag)) {
				extTags.add(newTag);
				ruleCheckMap.put(newTag, null);
			}
			newTag = matchIndexTag + validChars.charAt(i);
			if (!ruleCheckMap.containsKey(newTag)) {
				extTags.add(newTag);
				ruleCheckMap.put(newTag, null);
			}
		}
		return extTags;
	}

	/**
	 * 
	 * @param node
	 */
	private void createUrlClassRulesTree(UrlClassRuleTreeNode node) {
		if (node == null) {
			return;
		}

		List<String> extTags = extendTag(node.getTag());
		UrlClassRuleTreeNode preNode = node.getFirstChild();
		UrlClassRuleTreeNode curNode = null;
		List<UrlClassRule> nodeRuleLst = node.getRules();
		if (extTags != null && extTags.size() > 0) {
			for (Object tag : extTags.toArray()) {
				List<UrlClassRule> curRulesLst = new ArrayList<UrlClassRule>();
				if (nodeRuleLst != null && nodeRuleLst.size() > 0) {
					for (UrlClassRule ruleObj : nodeRuleLst) {
						if (ruleObj == null || ruleObj.getRule() == null) {
							continue;
						}
						String rule = ruleObj.getRule();
						if (rule.indexOf((String) tag) != -1)// 规则含有规则索引标签，则跳过继续
						{
							if (curNode == null) {
								curNode = new UrlClassRuleTreeNode();
								curNode.setTag((String) tag);
								curNode.setDeep(node.getDeep() + 1);
							}
							curRulesLst.add(ruleObj);
							curNode.incrementRuleCounter();
						}
					}
				}

				//
				if (curNode != null) {
					curNode.setParent(node);
					curNode.setRules(curRulesLst);
					if (preNode != null)
						preNode.setSibling(curNode);
					else
						node.setFirstChild(curNode);
					preNode = curNode;
					curNode = null;
				}
				for (Object ruleObj : curRulesLst) {
					nodeRuleLst.remove(ruleObj);
				}
			}
		}

		if (preNode != null && nodeRuleLst != null && nodeRuleLst.size() > 0) {
			curNode = new UrlClassRuleTreeNode();
			curNode.setTag(node.getTag());
			curNode.setRules(nodeRuleLst);
			curNode.setParent(node);
			preNode.setSibling(curNode);
			curNode.setDeep(node.getDeep() + 1);
			curNode.setRuleCounter(curNode.getRuleCounter()
					+ nodeRuleLst.size());
		}

		UrlClassRuleTreeNode child = node.getFirstChild();
		if (child != null) {
			if (child.getSibling() == null) {
				node.setTag(child.getTag());
				node.setRules(child.getRules());
				node.setFirstChild(null);
				createUrlClassRulesTree(node);
			} else {
				while (child != null) {
					createUrlClassRulesTree(child);
					child = child.getSibling();
				}
			}
		}
	}

	/**
	 * @param line
	 * @return
	 */
	private UrlClassRule parseUrlClassRule(String line) {
		String[] inputSplitedFields = line.split(ruleDelimiter, -1);
		if (inputSplitedFields.length < 6) {
			return null;
		}
		String sampleRule = inputSplitedFields[UrlClassRule.RULE_FIELD_RULE_INDEX]
				.trim();
		if (sampleRule == null) {
			return null;
		}

		String classId = inputSplitedFields[UrlClassRule.RULE_FIELD_CLASS_ID_INDEX]
				.trim();
		if (classId == null || classId.length() == 0) {
			return null;
		}
		if (!validRuleCheckPatn.matcher(classId).matches()) {
			return null;
		}

		String typeId = inputSplitedFields[UrlClassRule.RULE_FIELD_MATCH_TYPE_INDEX]
				.trim();

		String matchName = inputSplitedFields[UrlClassRule.RULE_FIELD_MATCH_NAME_INDEX]
				.trim();

		String classLevel = inputSplitedFields[UrlClassRule.RULE_FIELD_CLASS_LEVEL_INDEX]
				.trim();

		String className = inputSplitedFields[UrlClassRule.RULE_FIELD_CLASS_NAME_INDEX]
				.trim();

		String keywordsStr = null;
		if (inputSplitedFields.length > 6) {
			keywordsStr = inputSplitedFields[UrlClassRule.RULE_FIELD_KEYWORDS_INDEX]
					.trim();
		}

		String[] keywords = null;
		if (UrlClassRule.RULE_MATCH_FULL.equals(typeId))// 0-full match
		{
			keywords = sampleRule.split(UrlClassRule.RULE_WILDCARD_SYMBOLS, -1);
		} else if (UrlClassRule.RULE_MATCH_LEFT.equals(typeId))
		// 1-left match
		{
			keywords = sampleRule.split(UrlClassRule.RULE_WILDCARD_SYMBOLS, -1);
		} else if (UrlClassRule.RULE_MATCH_RIGHT.equals(typeId))
		// 2-right match
		{
			keywords = sampleRule.split(UrlClassRule.RULE_WILDCARD_SYMBOLS, -1);
		} else if (UrlClassRule.RULE_MATCH_ANY.equals(typeId))
		// 3-any match
		{
			keywords = sampleRule.split(UrlClassRule.RULE_WILDCARD_SYMBOLS, -1);
		} else if (UrlClassRule.RULE_MATCH_LEFT_RIGHT.equals(typeId)) {
			// 4-left and right match
			keywords = sampleRule.split(UrlClassRule.RULE_WILDCARD_SYMBOLS, -1);
		} else if (UrlClassRule.RULE_MATCH_REGULAR.equals(typeId))
		// 5-regular match
		{
			if (keywordsStr != null)
				keywords = keywordsStr.split(
						UrlClassRule.RULE_KEYWORDS_DELIMITER, -1);
		} else {
			return null;
		}

		String ruleID = null;
		if (inputSplitedFields.length > 7) {
			ruleID = inputSplitedFields[UrlClassRule.RULE_FIELD_ID_INDEX]
					.trim();
		}

		int priority = 100;
		if (inputSplitedFields.length > 8) {
			String priorityStr = inputSplitedFields[UrlClassRule.RULE_FIELD_PRIORITY_INDEX]
					.trim();
			if (priorityStr != null && priorityStr.length() > 0) {
				priority = Integer.parseInt(priorityStr);
			}
		}

		UrlClassRule rule = createNewUrlClassRule(typeId);
		if (rule == null) {
			return null;
		}
		rule.setSource(line);
		rule.setTypeId(typeId);
		rule.setTypeName(matchName);
		rule.setKeywords(keywords);
		rule.setClassId(classId);
		rule.setLevel(classLevel);
		rule.setClassName(className);
		rule.setRuleId(ruleID);
		rule.setPriority(priority);

		if (UrlClassRule.RULE_MATCH_REGULAR.equals(typeId)) {
			// .replace("*", ".*").replace("..", ".");
			try {
				Pattern urlPattern = Pattern.compile(sampleRule,
						Pattern.CASE_INSENSITIVE);
				urlPattern.matcher("sampleRule");// for test the regular
				rule.setRulePattern(urlPattern);
				rule.setRule(sampleRule);
				return rule;
			} catch (Exception e) {
				return null;
			}
		} else {
			rule.setRule(sampleRule);
			return rule;
		}
	}

	private UrlClassRule createNewUrlClassRule(String typeId) {
		switch (typeId) {
		case UrlClassRule.RULE_MATCH_FULL:
			return new FullMatchRule();
		case UrlClassRule.RULE_MATCH_ANY:
			return new AnyMatchRule();
		case UrlClassRule.RULE_MATCH_LEFT:
			return new LeftMatchRule();
		case UrlClassRule.RULE_MATCH_RIGHT:
			return new RightMatchRule();
		case UrlClassRule.RULE_MATCH_LEFT_RIGHT:
			return new LeftRightMatchRule();
		case UrlClassRule.RULE_MATCH_REGULAR:
			return new SimpleRegularMatchRule();
		}
		return null;
	}

	/**
	 * 
	 * @param rule
	 */
	private void cacheUrlClassRule(UrlClassRule rule) {
		if (rule == null || ruleListMap == null) {
			return;
		}

		String classLevel = rule.getLevel();
		if (classLevel == null) {
			return;
		}

		String matchType = rule.getTypeId();
		if (matchType == null) {
			return;
		}

		String sampleUrl = rule.getRule();
		if (sampleUrl == null) {
			return;
		}

		// 1. calc the rule key
		String ruleKey = calcRuleKey(sampleUrl, matchType);
		if (ruleKey == null) {
			return;
		}

		List<UrlClassRule> urlRulesOfLevel = null;
		if (ruleListMap.containsKey(classLevel)) {
			urlRulesOfLevel = (List<UrlClassRule>) ruleListMap.get(classLevel);
		}
		if (urlRulesOfLevel == null) {
			urlRulesOfLevel = new ArrayList<UrlClassRule>(1000);
		}

		// 3. calc the cache position
		int insertIndex = calcInsertIndex(urlRulesOfLevel, rule);
		if (insertIndex > -1) {
			urlRulesOfLevel.add(insertIndex, rule);
		} else {
			urlRulesOfLevel.add(rule);
		}

		ruleListMap.put(classLevel, urlRulesOfLevel);
	}

	/**
	 * 
	 * @param url
	 * @return
	 */
	private String calcRuleKey(String rule, String matchType) {
		if (rule == null || rule.length() == 0 || matchType == null) {
			return null;
		}
		String ruleIndexKey = null;
		if (UrlClassRule.RULE_MATCH_FULL.equalsIgnoreCase(matchType)) {
			ruleIndexKey = rule;
		} else if (UrlClassRule.RULE_MATCH_LEFT.equalsIgnoreCase(matchType)) {
			ruleIndexKey = rule.substring(0, adjustRuleKeyLength(rule));
		} else if (UrlClassRule.RULE_MATCH_RIGHT.equalsIgnoreCase(matchType)) {
			ruleIndexKey = rule.substring(rule.length()
					- adjustRuleKeyLength(rule));
		} else if (UrlClassRule.RULE_MATCH_ANY.equalsIgnoreCase(matchType)) {
			ruleIndexKey = "__match any__";
		} else if (UrlClassRule.RULE_MATCH_LEFT_RIGHT
				.equalsIgnoreCase(matchType)) {
			ruleIndexKey = rule.substring(0, RULE_INDEX_KEY_LENGTH) + "|"
					+ rule.substring(rule.length() - RULE_INDEX_KEY_LENGTH);

		} else if (UrlClassRule.RULE_MATCH_REGULAR.equalsIgnoreCase(matchType)) {
			ruleIndexKey = "__match regular__";
		}
		return ruleIndexKey;
	}

	/**
	 * 
	 * @return
	 */
	private int adjustRuleKeyLength(String rule) {
		if (rule == null) {
			return 0;
		}
		int ruleLen = rule.length();
		if (ruleLen < 1) {
			return 0;
		}

		if (ruleLen < RULE_INDEX_KEY_LENGTH)
			return ruleLen;
		else
			return RULE_INDEX_KEY_LENGTH;
	}

	/**
	 * 
	 * @param urlRulesOfLevel
	 * @param rule
	 * @return
	 */
	private int calcInsertIndex(List<UrlClassRule> urlRulesOfLevel,
			UrlClassRule rule) {
		if (urlRulesOfLevel == null || rule == null) {
			return -1;
		}

		int insertIndex = -1;
		int ruleCount = urlRulesOfLevel.size();
		for (int i = 0; i < ruleCount; i++) {
			UrlClassRule oldRule = urlRulesOfLevel.get(i);
			if (oldRule == null) {
				continue;
			}
			if (rule.getPriority() < oldRule.getPriority()) {
				return i;
			}
		}
		return insertIndex;
	}

	/**
	 * 
	 * @param prefix
	 */
	private void printJVMInfo(String prefix) {
		long tmem = Runtime.getRuntime().totalMemory();
		long fmem = Runtime.getRuntime().freeMemory();
		long mmem = Runtime.getRuntime().maxMemory();
		logger.info(prefix + " mem total=" + tmem / 1024 / 1024 + " MB");
		logger.info(prefix + " mem free=" + fmem / 1024 / 1024 + " MB");
		logger.info(prefix + " mem max=" + mmem / 1024 / 1024 + " MB");
	}
	

	@Override
	@JsonIgnore
	public DpiRule getDpiRule() {
		return dpiRule;
	}

	@Override
	@JsonIgnore
	public String getStatInfo() {
		ruleCounter = RuleCounter.getInstance();
		return ruleCounter.getUrlClassStatInfo();
	}

	@Override
	@JsonIgnore
	public Map<String, Long> getRuleMatchedStats() {
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

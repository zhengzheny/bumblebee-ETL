package com.gsta.bigdata.etl.core.function;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.w3c.dom.Element;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.ChildrenTag;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.GeneralRuleMgr;
import com.gsta.bigdata.etl.core.IRuleMgr;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.etl.core.ShellContext;
import com.gsta.bigdata.etl.core.function.dpi.RuleMatchCounter;
import com.gsta.bigdata.etl.core.function.dpi.UrlClassRule;
import com.gsta.bigdata.etl.core.function.dpi.UrlClassRuleManager;
import com.gsta.bigdata.etl.core.function.dpi.UrlClassRuleTreeNode;
import com.gsta.bigdata.etl.core.function.dpi.UrlInfo;
import com.gsta.bigdata.etl.core.function.dpi.UrlSplitter;

/**
 * 
 * @author shine
 *
 */
@SuppressWarnings("unused")
public class GetURLClass extends AbstractFunction {
	private static final long serialVersionUID = -6865479793149590382L;

	@JsonProperty
	private String inputField;
	@JsonProperty
	private String refUrlField;
	private String ruleFilePath;
	private String ref;

	@JsonProperty
	private UrlClassRuleManager ruleManager;

	private RuleMatchCounter ruleCounter = new RuleMatchCounter();

	private	long inputCounter = 0;		
	private	long matchingCounter = 0;		
	private	long matchedCounter = 0;		
	private	long invalidCounter = 0;	
	private	long unmatchedCounter = 0;
	private long level0Counter = 0;
	private long level1Counter = 0;
	private long level2Counter = 0;
	private long level3Counter = 0;
	private long level4Counter = 0;
	private long level5Counter = 0;
	private long level6Counter = 0;
	private long levelspamCounter = 0;

	@JsonProperty
	private Map<String, Long> ruleMatchedStatsMap = new HashMap<String, Long>(
			10000);

	public GetURLClass() {
		super();

		super.registerChildrenTags(new ChildrenTag(Constants.PATH_DPI_RULE,
				ChildrenTag.NODE));
	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		super.initAttrs(element);

		this.inputField = super.getAttr(Constants.ATTR_INPUT);
		this.refUrlField = super.getAttr(Constants.ATTR_REFURL);
		this.ref = super.getAttr(Constants.ATTR_REF);
	}

	@Override
	public void init(Element element) throws ParseException {
		super.init(element);

		IRuleMgr mgr = GeneralRuleMgr.getInstance().getRuleMgrById(this.ref);
		if (mgr instanceof UrlClassRuleManager) {
			this.ruleManager = (UrlClassRuleManager) mgr;
		}
		if (ruleManager == null) {
			throw new ParseException("can't find rule:" + ref);
		}

		try {
			ruleManager.init();
		} catch (IOException e) {
			throw new ParseException(e);
		}
	}

	@Override
	public String onCalculate(Map<String, String> functionData,
			ShellContext context) throws ETLException {
		return null;
	}

	@Override
	public Map<String, String> multiOutputOnCalculate(
			Map<String, String> functionData, ShellContext context)
			throws ETLException {
		String url = functionData.get(inputField);
		String ref = functionData.get(refUrlField);

		if (url == null || ref == null) {
			return null;
		}

		UrlInfo urlInfo = UrlSplitter.split(url);
		UrlInfo refInfo = UrlSplitter.split(ref);

		inputCounter++;

		if (urlInfo == null || urlInfo.getUrl() == null) {
			invalidCounter++;
			return null;
		}

		Map<String, String> retMap = new HashMap<String, String>();
		List<String> outputIds = super.getOutputIds();

		UrlClassRule matchedClassRule = this.lookupUrlClassRule(urlInfo,
				refInfo);
		if (matchedClassRule == null) {
			for (int i = 0; i < outputIds.size(); i++) {
				unmatchedCounter++;
				retMap.put(outputIds.get(i), null);
			}
		} else {
			for (int i = 0; i < outputIds.size(); i++) {
				if (i == 0) {
					retMap.put(outputIds.get(i), matchedClassRule.getClassId());
				} else if (i == 1) {
					retMap.put(outputIds.get(i), matchedClassRule.getRuleId());
				}
			}

			String ruleSource = matchedClassRule.getSource();
			long matchedCounter = 0;
			if (ruleMatchedStatsMap.containsKey(ruleSource)) {
				matchedCounter = ruleMatchedStatsMap.get(ruleSource) + 1;
			} else {
				matchedCounter = 1;
			}
			ruleMatchedStatsMap.put(ruleSource, matchedCounter);
		}
		
		//set statInfo
		String statInfo = getStatInfo();
		ruleManager.setStatInfo(statInfo);

		return retMap;
	}

	private String getStatInfo() {
		String statInfo = "\r\n";
		matchedCounter = levelspamCounter + level0Counter + level1Counter
				+ level2Counter + level3Counter + level4Counter + level5Counter
				+ level6Counter;
		statInfo += "--total input records: " + inputCounter + "\r\n";
		statInfo += "--total invalid records: " + invalidCounter + "\r\n";
		statInfo += "--total unmatched records: " + unmatchedCounter + "\r\n";
		statInfo += "--total matched records: " + matchedCounter + "\r\n";
		statInfo += "----level-spam matched: " + levelspamCounter + "\r\n";
		statInfo += "----level-0 matched: " + level0Counter + "\r\n";
		statInfo += "----level-1 matched: " + level1Counter + "\r\n";
		statInfo += "----level-2 matched: " + level2Counter + "\r\n";
		statInfo += "----level-3 matched: " + level3Counter + "\r\n";
		statInfo += "----level-4 matched: " + level4Counter + "\r\n";
		statInfo += "--total matching times: " + matchingCounter + "\r\n";
		
		return statInfo;
	}

	/**
	 * lookup the URL class ID
	 * 
	 * @param url
	 * @param referer
	 * @return
	 */
	private UrlClassRule lookupUrlClassRule(UrlInfo urlInfo, UrlInfo refInfo) {
		UrlClassRule result = null;

		// 1. check for spam
		result = lookupUrlClassIDByClassLevel(urlInfo, refInfo,
				UrlClassRule.RULE_LEVEL_SPAM);
		// match and filter the garbage url
		if (result != null) {
			levelspamCounter++;
			return result;
		}

		// 2. lookup in 4 level
		result = lookupUrlClassIDByClassLevel(urlInfo, refInfo,
				UrlClassRule.RULE_LEVEL_4);
		if (result != null) {
			level4Counter++;
			return result;
		}

		// 3. loopup in 3 level
		result = lookupUrlClassIDByClassLevel(urlInfo, refInfo,
				UrlClassRule.RULE_LEVEL_3);
		if (result != null) {
			level3Counter++;
			return result;
		}

		// 4. loopup in 2 level
		result = lookupUrlClassIDByClassLevel(urlInfo, refInfo,
				UrlClassRule.RULE_LEVEL_2);
		if (result != null) {
			level2Counter++;
			return result;
		}

		// 5. loopup in 1 level
		result = lookupUrlClassIDByClassLevel(urlInfo, refInfo,
				UrlClassRule.RULE_LEVEL_1);
		if (result != null) {
			level1Counter++;
			return result;
		}

		// 6. loopup in 0 level
		result = lookupUrlClassIDByClassLevel(urlInfo, refInfo,
				UrlClassRule.RULE_LEVEL_0);
		if (result != null) {
			level0Counter++;
			return result;
		}

		// 7. unknow level
		return null;
	}

	/**
	 * @param url
	 * @return String
	 */
	private UrlClassRule lookupUrlClassIDByClassLevel(UrlInfo urlInfo,
			UrlInfo refInfo, String classLevel) {
		if (classLevel == null || this.ruleManager == null) {
			return null;
		}

		String url = null;
		if (urlInfo != null) {
			url = urlInfo.getUrl();
		}
		String referer = null;
		if (refInfo != null) {
			referer = refInfo.getUrl();
		}
		if (url == null && referer == null) {
			return null;
		}

		UrlClassRuleTreeNode ruleTreeNode = ruleManager.getRuleTree(classLevel);
		if (ruleTreeNode == null) {
			return null;
		}

		ruleCounter.reset();
		UrlClassRule matchedRule = this.searchTree(ruleTreeNode,
				urlInfo.getUrl(), ruleCounter);
		matchingCounter += ruleCounter.totalCount;
		if (matchedRule != null) {
			switch (classLevel) {
			case UrlClassRule.RULE_LEVEL_SPAM:
				levelspamCounter++;
			case UrlClassRule.RULE_LEVEL_0:
				level0Counter++;
			case UrlClassRule.RULE_LEVEL_1:
				level1Counter++;
			case UrlClassRule.RULE_LEVEL_2:
				level2Counter++;
			case UrlClassRule.RULE_LEVEL_3:
				level3Counter++;
			case UrlClassRule.RULE_LEVEL_4:
				level4Counter++;
			case UrlClassRule.RULE_LEVEL_5:
				level5Counter++;
			case UrlClassRule.RULE_LEVEL_6:
				level6Counter++;
			}
		}
		return matchedRule;
	}

	/**
	 * @param node
	 * @param searchStr
	 * @param num
	 * @return
	 */
	private UrlClassRule searchTree(UrlClassRuleTreeNode node,
			String searchStr, RuleMatchCounter rmc) {
		if (node == null || searchStr == null) {
			return null;
		}
		// System.out.println("tag=" + node.getTag());

		String tag = node.getTag();
		rmc.totalCount += 1;
		if (searchStr.indexOf(tag) == -1) {
			return null;
		}

		UrlClassRuleTreeNode childNode = node.getFirstChild();
		if (childNode != null) {
			while (childNode != null) {
				// rmc.totalCount += 1;
				UrlClassRule rule = searchTree(childNode, searchStr, rmc);
				if (rule != null)
					return rule;
				else
					childNode = childNode.getSibling();
			}
		} else {
			List<UrlClassRule> rules = node.getRules();
			if (rules != null) {
				for (UrlClassRule rule : rules) {
					// System.out.println("\t" + rule.getRule());
					rmc.totalCount += 1;
					if (rule.isMatch(searchStr)) {
						return rule;
					}
				}
			}
			return null;
		}

		return null;
	}

}

package com.gsta.bigdata.etl.core.function;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.w3c.dom.Element;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.GeneralRuleMgr;
import com.gsta.bigdata.etl.core.IRuleMgr;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.etl.core.ShellContext;
import com.gsta.bigdata.etl.core.function.dpi.RuleCounter;
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
	private String refRule;
	
	@JsonProperty
	private RuleCounter ruleCounter;
	@JsonProperty
	private List<String> outputIdList;
	
	@JsonProperty
	private UrlClassRuleManager ruleManager;

	private RuleMatchCounter ruleMatchCounter = new RuleMatchCounter();

	@JsonProperty
	private Map<String, Long> ruleMatchedStatsMap = new ConcurrentHashMap<String, Long>(
			10000);

	public GetURLClass() {
		super();

	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		super.initAttrs(element);

		this.inputField = super.getAttr(Constants.ATTR_INPUT);
		this.refUrlField = super.getAttr(Constants.ATTR_REFURL);
		this.refRule = super.getAttr(Constants.ATTR_REFRULE);
	}

	@Override
	public void init(Element element) throws ParseException {
		super.init(element);
		
		ruleCounter = RuleCounter.getInstance();
		
		this.outputIdList = super.getOutputIds();
		int outSize = this.outputIdList.size();
		if(outSize != 2){
			throw new ParseException("GetUrlClass function must have 2 output value");
		}

		IRuleMgr mgr = GeneralRuleMgr.getInstance().getRuleMgrById(this.refRule);
		if (mgr instanceof UrlClassRuleManager) {
			this.ruleManager = (UrlClassRuleManager) mgr;
		}
		if (ruleManager == null) {
			throw new ParseException("can't find rule:" + refRule);
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

		ruleCounter.setInputCounter();

		if (urlInfo == null || urlInfo.getUrl() == null) {
			ruleCounter.setInvalidCounter();
			return null;
		}

		Map<String, String> retMap = new HashMap<String, String>();
		UrlClassRule matchedClassRule = this.lookupUrlClassRule(urlInfo,
				refInfo);
		if (matchedClassRule == null) {
			for (int i = 0; i < outputIdList.size(); i++) {
				ruleCounter.setUnmatchedCounter();
				retMap.put(outputIdList.get(i), null);
			}
		} else {
			retMap.put(outputIdList.get(0), matchedClassRule.getClassId());
			retMap.put(outputIdList.get(1), matchedClassRule.getRuleId());

			String ruleSource = matchedClassRule.getSource();
			long matchedCounter = 0;
			if (ruleMatchedStatsMap.containsKey(ruleSource)) {
				matchedCounter = ruleMatchedStatsMap.get(ruleSource) + 1;
			} else {
				matchedCounter = 1;
			}
			ruleMatchedStatsMap.put(ruleSource, matchedCounter);
		}
		
		return retMap;
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
			ruleCounter.setLevelspamCounter();
			return result;
		}

		// 2. lookup in 4 level
		result = lookupUrlClassIDByClassLevel(urlInfo, refInfo,
				UrlClassRule.RULE_LEVEL_4);
		if (result != null) {
			ruleCounter.setLevel4Counter();
			return result;
		}

		// 3. loopup in 3 level
		result = lookupUrlClassIDByClassLevel(urlInfo, refInfo,
				UrlClassRule.RULE_LEVEL_3);
		if (result != null) {
			ruleCounter.setLevel3Counter();
			return result;
		}

		// 4. loopup in 2 level
		result = lookupUrlClassIDByClassLevel(urlInfo, refInfo,
				UrlClassRule.RULE_LEVEL_2);
		if (result != null) {
			ruleCounter.setLevel2Counter();
			return result;
		}

		// 5. loopup in 1 level
		result = lookupUrlClassIDByClassLevel(urlInfo, refInfo,
				UrlClassRule.RULE_LEVEL_1);
		if (result != null) {
			ruleCounter.setLevel1Counter();
			return result;
		}

		// 6. loopup in 0 level
		result = lookupUrlClassIDByClassLevel(urlInfo, refInfo,
				UrlClassRule.RULE_LEVEL_0);
		if (result != null) {
			ruleCounter.setLevel0Counter();
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

		ruleMatchCounter.reset();
		UrlClassRule matchedRule = this.searchTree(ruleTreeNode,
				urlInfo.getUrl(), ruleMatchCounter);
		ruleCounter.setMatchingCounter(ruleMatchCounter.totalCount.get());
		if (matchedRule != null) {
			switch (classLevel) {
			case UrlClassRule.RULE_LEVEL_SPAM:
				ruleCounter.setLevelspamCounter();
			case UrlClassRule.RULE_LEVEL_0:
				ruleCounter.setLevel0Counter();
			case UrlClassRule.RULE_LEVEL_1:
				ruleCounter.setLevel1Counter();
			case UrlClassRule.RULE_LEVEL_2:
				ruleCounter.setLevel2Counter();
			case UrlClassRule.RULE_LEVEL_3:
				ruleCounter.setLevel3Counter();
			case UrlClassRule.RULE_LEVEL_4:
				ruleCounter.setLevel4Counter();
			case UrlClassRule.RULE_LEVEL_5:
				ruleCounter.setLevel5Counter();
			case UrlClassRule.RULE_LEVEL_6:
				ruleCounter.setLevel6Counter();
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

		String tag = node.getTag();
		rmc.totalCount.getAndIncrement();
		if (searchStr.indexOf(tag) == -1) {
			return null;
		}

		UrlClassRuleTreeNode childNode = node.getFirstChild();
		if (childNode != null) {
			while (childNode != null) {
				rmc.totalCount.getAndIncrement();
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
					rmc.totalCount.getAndIncrement();
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

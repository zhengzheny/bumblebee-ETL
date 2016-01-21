package com.gsta.bigdata.etl.core.function;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.GeneralRuleMgr;
import com.gsta.bigdata.etl.core.IRuleMgr;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.etl.core.ShellContext;
import com.gsta.bigdata.etl.core.function.dpi.MatchedUseragent;
import com.gsta.bigdata.etl.core.function.dpi.RuleCounter;
import com.gsta.bigdata.etl.core.function.dpi.TerminalInfo;
import com.gsta.bigdata.etl.core.function.dpi.UniIDGenerator;
import com.gsta.bigdata.etl.core.function.dpi.UserAgentRuleManager;
import com.gsta.bigdata.etl.core.function.dpi.UseragentCacheCleaner;
import com.gsta.bigdata.etl.core.function.dpi.UseragentParserRule;
import com.gsta.bigdata.utils.FileUtils;

/**
 * 
 * @author shine
 *
 */

public class ParseUserAgent extends AbstractFunction {
	private static final long serialVersionUID = -2615428311696520587L;

	@JsonProperty
	private Map<String, MatchedUseragent> useragentCacheMap;
/*	@JsonProperty
	private Map<String, Long> ruleStatMap;*/

	private UseragentCacheCleaner cacheCleanThread;
	@JsonProperty
	private UserAgentRuleManager ruleManager;

	private String refRule;
	private int useragentCacheSize;

	@JsonProperty
	private String inputField;

	@JsonIgnore
	private Logger logger = LoggerFactory.getLogger(getClass());

	@JsonProperty
	private RuleCounter ruleCounter;
	@JsonProperty
	private UseragentParserRule[] rules;
	@JsonProperty
	private List<String> outputIdList;

	private int cacheSize;
	private int cleanInterval;
	private float cleanRatio;

	public static final int RULE_MATCHED = 1;
	public static final int RULE_UNMATCHED = 0;
	public final static String ATTR_SIZE = "size";
	public final static String ATTR_CLEANINTERVAL = "cleanInterval";
	public final static String ATTR_CLEANRATIO = "cleanRatio";

	private static final String RULE_KEY_PREFIX = "rule@";

	public ParseUserAgent() {
		super();

	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		super.initAttrs(element);

		this.inputField = super.getAttr(Constants.ATTR_INPUT);
		this.refRule = super.getAttr(Constants.ATTR_REFRULE);
	}

	@Override
	public void init(Element element) throws ParseException {
		super.init(element);
		// init RuleCounter
		this.ruleCounter = RuleCounter.getInstance();
		
		this.outputIdList = super.getOutputIds();
		int outSize = this.outputIdList.size();
		if (outSize != 12) {
			throw new ParseException(
					"ParseUserAgent function must have 12 output value");
		}

		GeneralRuleMgr generalRuleMgr = GeneralRuleMgr.getInstance();
		IRuleMgr mgr = generalRuleMgr.getRuleMgrById(this.refRule);
		if (mgr instanceof UserAgentRuleManager) {
			this.ruleManager = (UserAgentRuleManager) mgr;
		}
		if (this.ruleManager == null) {
			throw new ParseException("can't find rule:" + refRule);
		}

		ruleManager.init();

		this.rules = ruleManager.getRules();
		if (this.rules == null) {
			throw new ParseException("can't find any UserAgent rule");
		}

		String filePath = generalRuleMgr.getDpiRuleById(this.refRule)
				.getFilePath();
		this.getDpiCacheByFilePath(filePath);

		//this.ruleStatMap = new ConcurrentHashMap<String, Long>();
		useragentCacheSize = this.cacheSize;
		if (useragentCacheSize >= 0) {
			useragentCacheMap = new HashMap<String, MatchedUseragent>(
					useragentCacheSize);
			cacheCleanThread = new UseragentCacheCleaner(useragentCacheMap,
					useragentCacheSize, this.cleanInterval, this.cleanRatio);
			new Thread(cacheCleanThread).start();
		}
	}

	private void getDpiCacheByFilePath(String filePath) {
		if (StringUtils.isBlank(filePath)) {
			return;
		}

		try {
			InputStream input = FileUtils.getInputFile(filePath);
			Properties properties = new Properties();
			properties.load(input);

			String size = properties.getProperty(ATTR_SIZE, "0");
			String cleanInterval = properties.getProperty(ATTR_CLEANINTERVAL,
					"0");
			String cleanRatio = properties.getProperty(ATTR_CLEANRATIO, "0");

			this.cacheSize = Integer.parseInt(size);
			this.cleanInterval = Integer.parseInt(cleanInterval);
			this.cleanRatio = Float.parseFloat(cleanRatio);
		} catch (Exception e) {
			logger.error("can't find the file path:" + filePath);
			return;
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
		String useragent = functionData.get(inputField);
		if (useragent == null || ruleManager == null) {
			return null;
		}

		TerminalInfo terminalInfo = parseRules(useragent, this.rules);
		Map<String, String> retMap = new HashMap<String, String>();

		if (terminalInfo == null) {
			for (int i = 0; i < outputIdList.size(); i++) {
				retMap.put(outputIdList.get(i), null);
			}
		} else {
			retMap.put(outputIdList.get(0), terminalInfo.getAppName());
			retMap.put(outputIdList.get(1), terminalInfo.getAppVer());
			retMap.put(outputIdList.get(2), terminalInfo.getTerminalBrand());
			retMap.put(outputIdList.get(3), terminalInfo.getTerminalType());
			retMap.put(outputIdList.get(4), terminalInfo.getCharset());
			retMap.put(outputIdList.get(5), terminalInfo.getBrowser());
			retMap.put(outputIdList.get(6), terminalInfo.getOsVer());
			retMap.put(outputIdList.get(7), terminalInfo.getOsName());
			retMap.put(outputIdList.get(8),
					String.valueOf(terminalInfo.getTerminalCategory()));
			retMap.put(outputIdList.get(9), UniIDGenerator.generateUniqueID());
			retMap.put(outputIdList.get(10),
					String.valueOf(terminalInfo.getMatchedFlag()));
			retMap.put(outputIdList.get(11), terminalInfo.getRule().getId());
		}

		// set rulestatMap
		//ruleManager.setRuleMatchedStats(ruleStatMap);

		return retMap;
	}

	/**
	 * @param useragent
	 * @param rules
	 * @return
	 */
	private TerminalInfo parseRules(String useragent,
			UseragentParserRule[] rules) {
		if (useragent == null || rules == null || rules.length == 0
				|| ruleManager == null) {
			return null;
		}
		ruleCounter.setnTotalRecondsForRead();

		String fmtUA = useragent.trim();

		int uaLen = fmtUA.length();
		if (uaLen < 5) {
			ruleCounter.setnSkipRecordsForInvalidLen();
			return null;
		}

		Pattern keywordPat = ruleManager.getUseragentKeywordPattern();
		if (keywordPat != null && !keywordPat.matcher(fmtUA).find()) {
			ruleCounter.setnSkipRecordsForNoKeywords();
			return null;
		}

		TerminalInfo terminalInfo = lookupCache(fmtUA);
		if (terminalInfo != null) {
			UseragentParserRule rule = terminalInfo.getRule();
			if (rule != null) {
				this.updateRuleStat(rule.getRegular());
			}
			ruleCounter.setnUpdatedRecords();
			return terminalInfo;
		}

		int ruleCount = rules.length;
		Matcher matcher = null;
		for (int i = 0; i < ruleCount; i++) {
			UseragentParserRule rule = rules[i];
			if (rule == null || rule.getPattern() == null) {
				continue;
			}
			// matcher = rule.getPattern().matcher(fmtUA);
			String regular = rule.getRegular();
			Pattern pattern = Pattern
					.compile(regular, Pattern.CASE_INSENSITIVE);
			matcher = pattern.matcher(fmtUA);
			if (matcher == null) {
				continue;
			}

			if (matcher.find()) {
				terminalInfo = fillTerminalInfo(rule, matcher);
				cacheParsedTerminalInfo(fmtUA, terminalInfo);
				this.updateRuleStat(RULE_KEY_PREFIX + rule.getId());
				return terminalInfo;
			}
		}

		ruleCounter.setnUnmatchedRecords();
		return null;
	}

	/**
	 * @param useragent
	 * @return
	 */
	private TerminalInfo lookupCache(String useragent) {
		if (useragent == null || useragentCacheMap == null) {
			return null;
		}

		TerminalInfo terminalInfo = null;
		if (this.useragentCacheMap.containsKey(useragent)) {
			MatchedUseragent cachedUA = (MatchedUseragent) useragentCacheMap
					.get(useragent);
			if (cachedUA != null) {
				terminalInfo = cachedUA.getTerminalInfo();
				cachedUA.setUpdateTime(System.currentTimeMillis());
				useragentCacheMap.put(useragent, cachedUA);
			}
		}

		return terminalInfo;
	}

	/**
	 * @param useragent
	 */
	private void updateRuleStat(String ruleReg) {
		if (ruleReg == null) {
			return;
		}
		
		Map<String,Long> ruleStatMap = this.ruleManager.getRuleMatchedStats();
		if (ruleStatMap == null) {
			ruleStatMap = new ConcurrentHashMap<String/* rule */, Long/* counter */>();
		}

		Long ruleCounter = (Long) ruleStatMap.get(ruleReg);
		if (ruleCounter == null) {
			ruleCounter = 1L;
		} else {
			ruleCounter += 1;
		}
		//ruleStatMap.put(ruleReg, ruleCounter);
		this.ruleManager.getRuleMatchedStats().put(ruleReg, ruleCounter);
	}

	/**
	 * @param rule
	 * @param matcher
	 * @return
	 */
	private TerminalInfo fillTerminalInfo(UseragentParserRule rule,
			Matcher matcher) {
		TerminalInfo info = new TerminalInfo();
		info.setMatchedFlag(RULE_MATCHED);
		info.setRule(rule);
		int[] outputIndexes = rule.getIndexes();
		int matchCount = matcher.groupCount();

		// app name
		if (outputIndexes[0] > -1 && outputIndexes[0] <= matchCount) {
			info.setAppName(format(matcher.group(outputIndexes[0])));
		}

		// app version
		if (outputIndexes[1] > -1 && outputIndexes[1] <= matchCount) {
			info.setAppVer(format(matcher.group(outputIndexes[1])));
		}

		// setTerminalBrand
		if (outputIndexes[2] > -1 && outputIndexes[2] <= matchCount) {
			info.setTerminalBrand(format(matcher.group(outputIndexes[2])));
		}

		// setTerminalType
		if (outputIndexes[3] > -1 && outputIndexes[3] <= matchCount) {
			info.setTerminalType(format(matcher.group(outputIndexes[3])));
		}

		// setCharset
		if (outputIndexes[4] > -1 && outputIndexes[4] <= matchCount) {
			info.setCharset(format(matcher.group(outputIndexes[4])));
		}

		// browser
		if (outputIndexes[5] > -1 && outputIndexes[5] <= matchCount) {
			info.setBrowser(format(matcher.group(outputIndexes[5])));
		}

		// setOsVer
		if (outputIndexes[6] > -1 && outputIndexes[6] <= matchCount) {
			info.setOsVer(format(matcher.group(outputIndexes[6])));
		}

		// setOsName
		if (outputIndexes[7] > -1 && outputIndexes[7] <= matchCount) {
			info.setOsName(format(matcher.group(outputIndexes[7])));
		}

		return lookupTerminalInfo(info);
	}

	/**
	 * @param useragent
	 * @param tinfo
	 */
	private void cacheParsedTerminalInfo(String useragent, TerminalInfo tinfo) {
		if (useragentCacheMap != null
				&& useragentCacheMap.size() < this.useragentCacheSize) {
			MatchedUseragent mua = new MatchedUseragent();
			mua.setTerminalInfo(tinfo);
			mua.setUpdateTime(System.currentTimeMillis());
			useragentCacheMap.put(useragent, mua);
			ruleCounter.setnCachedRecords();
		}
	}

	/**
	 * @param info
	 * @return
	 */
	private TerminalInfo lookupTerminalInfo(TerminalInfo info) {
		return info;
	}

	/**
	 * 
	 * @param data
	 * @return
	 */
	public String format(String data) {
		if (data == null) {
			return null;
		}
		String fmted = data.trim().toLowerCase();
		if (fmted.indexOf("_") > -1)
			fmted = fmted.replaceAll("_", " ");
		if (fmted.indexOf("/") > -1)
			fmted = fmted.replaceAll("/", " ");
		if (fmted.indexOf("\\") > -1)
			fmted = fmted.replaceAll("\\\\", " ");
		if (fmted.indexOf("-") > -1)
			fmted = fmted.replaceAll("\\-", " ");
		if (fmted.indexOf("\t") > -1)
			fmted = fmted.replaceAll("\\t", " ");
		if (fmted.indexOf("  ") > -1)
			fmted = fmted.replaceAll("\\s{2,}", " ");
		return fmted.trim();
	}
}

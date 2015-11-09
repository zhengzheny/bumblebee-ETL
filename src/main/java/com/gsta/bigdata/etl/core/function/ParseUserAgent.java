package com.gsta.bigdata.etl.core.function;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.ChildrenTag;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.GeneralRuleMgr;
import com.gsta.bigdata.etl.core.IRuleMgr;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.etl.core.ShellContext;
import com.gsta.bigdata.etl.core.function.dpi.DpiCache;
import com.gsta.bigdata.etl.core.function.dpi.MatchedUseragent;
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
	@JsonProperty
	private Map<String, Long> ruleStatMap;

	private UseragentCacheCleaner cacheCleanThread;
	@JsonProperty
	private UserAgentRuleManager ruleManager;

	private String ref;
	private int useragentCacheSize;

	@JsonProperty
	private String inputField;

	@JsonIgnore
	private Logger logger = LoggerFactory.getLogger(getClass());

	private long nTotalRecondsForRead = 0;
	private long nSkipRecordsForInvalidLen = 0;
	private long nSkipRecordsForNoKeywords = 0;
	private long nUpdatedRecords = 0;
	private long nUnmatchedRecords = 0;
	private long nCachedRecords = 0;

	public static final int RULE_MATCHED = 1;
	public static final int RULE_UNMATCHED = 0;

	private static final String RULE_KEY_PREFIX = "rule@";

	public ParseUserAgent() {
		super();

		super.registerChildrenTags(new ChildrenTag(Constants.PATH_DPI_RULE,
				ChildrenTag.NODE));
		super.registerChildrenTags(new ChildrenTag(Constants.PATH_DPI_CACHE,
				ChildrenTag.NODE));
	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		super.initAttrs(element);

		this.inputField = super.getAttr(Constants.ATTR_INPUT);
		this.ref = super.getAttr(Constants.ATTR_REF);
	}

	@Override
	public void init(Element element) throws ParseException {
		super.init(element);

		IRuleMgr mgr = GeneralRuleMgr.getInstance().getRuleMgrById(this.ref);
		if (mgr instanceof UserAgentRuleManager) {
			this.ruleManager = (UserAgentRuleManager) mgr;
		}
		if (this.ruleManager == null) {
			throw new ParseException("can't find rule:" + ref);
		}

		ruleManager.init();

		String filePath = GeneralRuleMgr.getInstance().getDpiRuleById(this.ref)
				.getFilePath();
		DpiCache dpiCache = getDpiCacheByFilePath(filePath);

		int cacheSize = 0;
		int cleanInterval = 0;
		float cleanRatio = 0;
		if (dpiCache != null) {
			cacheSize = Integer.parseInt(dpiCache.getSize());
			cleanInterval = Integer.parseInt(dpiCache.getCleanInterval());
			cleanRatio = Float.parseFloat(dpiCache.getCleanRatio());
		}

		this.ruleStatMap = new HashMap<String, Long>();
		useragentCacheSize = cacheSize;
		if (useragentCacheSize >= 0) {
			useragentCacheMap = new HashMap<String, MatchedUseragent>(
					useragentCacheSize);
			cacheCleanThread = new UseragentCacheCleaner(useragentCacheMap,
					useragentCacheSize, cleanInterval, cleanRatio);
			new Thread(cacheCleanThread).start();
		}
	}

	private DpiCache getDpiCacheByFilePath(String filePath) {
		if (StringUtils.isBlank(filePath)) {
			return null;
		}

		DpiCache dpiCache = new DpiCache();
		try {
			InputStream input = FileUtils.getInputFile(filePath);
			Properties properties = new Properties();
			properties.load(input);

			String size = properties.getProperty(DpiCache.ATTR_SIZE, "0");
			String cleanInterval = properties.getProperty(
					DpiCache.ATTR_CLEANINTERVAL, "0");
			String cleanRatio = properties.getProperty(
					DpiCache.ATTR_CLEANRATIO, "0");

			dpiCache.setSize(size);
			dpiCache.setCleanInterval(cleanInterval);
			dpiCache.setCleanRatio(cleanRatio);
		} catch (Exception e) {
			logger.error("can't find the file path:" + filePath);
			return null;
		}

		return dpiCache;
	}

	@Override
	protected void createChildNode(Element node) throws ParseException {
		super.createChildNode(node);

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

		UseragentParserRule[] rules = ruleManager.getRules();
		if (rules == null) {
			return null;
		}

		TerminalInfo terminalInfo = parseRules(useragent, rules);
		List<String> outputIds = super.getOutputIds();
		Map<String, String> retMap = new HashMap<String, String>();

		if (terminalInfo == null) {
			for (int i = 0; i < outputIds.size(); i++) {
				retMap.put(outputIds.get(i), null);
			}
		} else {
			for (int i = 0; i < outputIds.size(); i++) {
				if (i == 0) {
					retMap.put(outputIds.get(i), terminalInfo.getAppName());
				} else if (i == 1) {
					retMap.put(outputIds.get(i), terminalInfo.getAppVer());
				} else if (i == 2) {
					retMap.put(outputIds.get(i),
							terminalInfo.getTerminalBrand());
				} else if (i == 3) {
					retMap.put(outputIds.get(i), terminalInfo.getTerminalType());
				} else if (i == 4) {
					retMap.put(outputIds.get(i), terminalInfo.getCharset());
				} else if (i == 5) {
					retMap.put(outputIds.get(i), terminalInfo.getBrowser());
				} else if (i == 6) {
					retMap.put(outputIds.get(i), terminalInfo.getOsVer());
				} else if (i == 7) {
					retMap.put(outputIds.get(i), terminalInfo.getOsName());
				} else if (i == 8) {
					retMap.put(outputIds.get(i),
							String.valueOf(terminalInfo.getTerminalCategory()));
				} else if (i == 9) {
					retMap.put(outputIds.get(i),
							UniIDGenerator.generateUniqueID());
				} else if (i == 10) {
					retMap.put(outputIds.get(i),
							String.valueOf(terminalInfo.getMatchedFlag()));
				} else if (i == 11) {
					retMap.put(outputIds.get(i), terminalInfo.getRule().getId());
				}
			}
		}

		// set statInfo
		String statInfo = getStatInfo();
		ruleManager.setStatInfo(statInfo);
		ruleManager.setRuleMatchedStats(ruleStatMap);

		return retMap;
	}

	private String getStatInfo() {
		String userAgentStatsMsg = "debug info------Useragent statistics in this mapper: \r\n"
				+ "\tTotal records:" + this.nTotalRecondsForRead + "\r\n" 
				+ "\tinvalid length records: " + this.nSkipRecordsForInvalidLen + "\r\n"
				+ "\tno keyword records: " + this.nSkipRecordsForNoKeywords + "\r\n"
				+ "\tunmatched records: " + this.nUnmatchedRecords + "\r\n"
				+ "\tcached records: " + this.nCachedRecords + "\r\n"
				+ "\tlookuped records: " + this.nUpdatedRecords + "\r\n"
				+ "\tcache map size: " + this.useragentCacheMap.size();
		return userAgentStatsMsg;
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
		this.nTotalRecondsForRead++;

		String fmtUA = useragent.trim();

		int uaLen = fmtUA.length();
		if (uaLen < 5) {
			this.nSkipRecordsForInvalidLen++;
			return null;
		}

		Pattern keywordPat = ruleManager.getUseragentKeywordPattern();
		if (keywordPat != null && !keywordPat.matcher(fmtUA).find()) {
			this.nSkipRecordsForNoKeywords++;
			return null;
		}

		TerminalInfo terminalInfo = lookupCache(fmtUA);
		if (terminalInfo != null) {
			UseragentParserRule rule = terminalInfo.getRule();
			if (rule != null) {
				this.updateRuleStat(rule.getRegular());
			}
			this.nUpdatedRecords++;
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

		this.nUnmatchedRecords++;
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
		if (ruleStatMap == null) {
			ruleStatMap = new HashMap<String/* rule */, Long/* counter */>();
		}

		Long ruleCounter = (Long) ruleStatMap.get(ruleReg);
		if (ruleCounter == null) {
			ruleCounter = 1L;
		} else {
			ruleCounter += 1;
		}
		ruleStatMap.put(ruleReg, ruleCounter);
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
			this.nCachedRecords++;
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

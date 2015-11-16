package com.gsta.bigdata.etl.core.function;

import java.net.URLDecoder;
import java.util.Map;
import java.util.regex.Matcher;

import org.w3c.dom.Element;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.GeneralRuleMgr;
import com.gsta.bigdata.etl.core.IRuleMgr;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.etl.core.ShellContext;
import com.gsta.bigdata.etl.core.function.dpi.SearchWordsRule;
import com.gsta.bigdata.etl.core.function.dpi.SearchWordsRuleManager;
import com.gsta.bigdata.utils.StringUtils;

/**
 * 
 * @author shine
 *
 */
public class ParseSearchKeywords extends AbstractFunction {
	private static final long serialVersionUID = -6233374356756838597L;

	@JsonProperty
	private String inputField;

	private String refRule;

	@JsonProperty
	private SearchWordsRuleManager ruleManager;

	private static final String ENCODING_GBK = "GBK";
	private static final String ENCODING_ISO88591 = "ISO-8859-1";
	private static final String ENCODING_GB2312 = "GB2312";
	private static final String ENCODING_BIG5 = "BIG5";

	private static final String PARAM_OUTPUT_FIELD_DELIMITER = "|";

	@Override
	protected void initAttrs(Element element) throws ParseException {
		super.initAttrs(element);

		this.inputField = super.getAttr(Constants.ATTR_INPUT);
		this.refRule = super.getAttr(Constants.ATTR_REFRULE);
	}

	@Override
	public void init(Element element) throws ParseException {
		super.init(element);

		IRuleMgr mgr = GeneralRuleMgr.getInstance().getRuleMgrById(this.refRule);
		if (mgr instanceof SearchWordsRuleManager) {
			this.ruleManager = (SearchWordsRuleManager) mgr;
		}
		if (this.ruleManager == null) {
			throw new ParseException("can't find rule:" + refRule);
		}

		ruleManager.init();
	}

	@Override
	public String onCalculate(Map<String, String> functionData,
			ShellContext context) throws ETLException {
		String url = functionData.get(inputField);

		if (url == null || ruleManager == null) {
			return null;
		}

		// 1.load rules
		SearchWordsRule[] rules = ruleManager.getRules();
		if (rules == null) {
			return null;
		}

		int ruleCount = rules.length;
		Matcher matcher = null;
		String searchKeywords = null;
		for (int i = 0; i < ruleCount; i++) {
			SearchWordsRule rule = rules[i];
			if (rule == null || rule.getPattern() == null) {
				continue;
			}
			matcher = rule.getPattern().matcher(url);
			if (matcher == null) {
				continue;
			}

			if (matcher.find()) {
				int groupCount = matcher.groupCount();
				int groupIndex = rule.getGroupIndex();
				if (groupIndex > 0 && groupIndex <= groupCount) {
					String word = matcher.group(groupIndex).trim();
					if (word.endsWith("%")) {
						word = word.substring(0, word.length() - 1);
					}
					searchKeywords = decodeKeywords(word, rule.getCharset(),
							rule.getDecoder());
					break;
				}
			}
		}

		if (searchKeywords != null) {
			searchKeywords = searchKeywords.replace(
					PARAM_OUTPUT_FIELD_DELIMITER, " ");
			searchKeywords = searchKeywords.replace("\r", " ");
			searchKeywords = searchKeywords.replace("\n", " ");
		}

		return searchKeywords;
	}

	@Override
	public Map<String, String> multiOutputOnCalculate(
			Map<String, String> functionData, ShellContext context)
			throws ETLException {
		return null;
	}

	/**
	 * decode the keywords
	 * 
	 * @return
	 */
	public String decodeKeywords(String word, String charset, String decoder) {
		if (word == null || word.length() == 0) {
			return null;
		}
		try {
			String result = URLDecoder.decode(word, Constants.DEFAULT_ENCODING);
			if (!StringUtils.isMessyCode(result)) {
				return result;
			}

			result = URLDecoder.decode(word, ENCODING_GBK);
			if (!StringUtils.isMessyCode(result)) {
				return result;
			}
			result = URLDecoder.decode(word, ENCODING_GB2312);

			if (!StringUtils.isMessyCode(result)) {
				return result;
			}

			result = URLDecoder.decode(word, ENCODING_ISO88591);
			if (!StringUtils.isMessyCode(result)) {
				return result;
			}

			result = URLDecoder.decode(word, ENCODING_BIG5);
			return result;
		} catch (Exception e) {
		}
		return null;
	}

}

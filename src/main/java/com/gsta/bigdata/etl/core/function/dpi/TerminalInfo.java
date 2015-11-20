package com.gsta.bigdata.etl.core.function.dpi;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author xiangy
 *
 */
public class TerminalInfo implements Serializable{
	private static final long serialVersionUID = 1978808497085749746L;
	
	@JsonProperty
	private String appName;			
	@JsonProperty
	private String appVer;			
	@JsonProperty
	private int terminalCategory;	
	@JsonProperty
	private String terminalBrand;	
	@JsonProperty
	private String terminalType;	
	@JsonProperty
	private String charset;			
	@JsonProperty
	private String browser;			
	@JsonProperty
	private String osName;			
	@JsonProperty
	private String osVer;			

	@JsonProperty
	private int matchedFlag;		
	@JsonProperty
	private UseragentParserRule rule;
	
	public TerminalInfo() {
	}

	/**
	 * @param appName
	 * @param appVer
	 * @param terminalBrand
	 * @param terminalType
	 * @param charset
	 * @param browser
	 * @param osVer
	 * @param osName
	 * @param terminalCategory
	 * @param matchedFlag
	 * @param rule
	 */
	public TerminalInfo(String appName, String appVer, String terminalBrand, String terminalType, String charset, 
			String browser, String osVer, String osName, int terminalCategory, int matchedFlag,UseragentParserRule rule)
	{
		this.appName = appName;
		this.appVer = appVer;
		this.terminalBrand = terminalBrand;
		this.terminalType = terminalType;
		this.charset = charset;
		this.browser = browser;
		this.osVer = osVer;
		this.osName = osName;
		this.terminalCategory = terminalCategory;
		this.matchedFlag = matchedFlag;
		this.rule = rule;
	}
	
	
	public UseragentParserRule getRule() {
		return rule;
	}

	public void setRule(UseragentParserRule rule) {
		this.rule = rule;
	}

	public int getMatchedFlag() {
		return matchedFlag;
	}

	public void setMatchedFlag(int matchedFlag) {
		this.matchedFlag = matchedFlag;
	}

	public String getAppName() {
		return appName;
	}

	public void setAppName(String appName) {
		this.appName = appName;
	}

	public String getAppVer() {
		return appVer;
	}

	public void setAppVer(String appVer) {
		this.appVer = appVer;
	}

	public int getTerminalCategory() {
		return terminalCategory;
	}

	public void setTerminalCategory(int terminalCategory) {
		this.terminalCategory = terminalCategory;
	}

	public String getTerminalBrand() {
		return terminalBrand;
	}

	public void setTerminalBrand(String terminalBrand) {
		this.terminalBrand = terminalBrand;
	}

	public String getTerminalType() {
		return terminalType;
	}

	public void setTerminalType(String terminalType) {
		this.terminalType = terminalType;
	}

	public String getCharset() {
		return charset;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	public String getBrowser() {
		return browser;
	}

	public void setBrowser(String browser) {
		this.browser = browser;
	}

	public String getOsName() {
		return osName;
	}

	public void setOsName(String osName) {
		this.osName = osName;
	}

	public String getOsVer() {
		return osVer;
	}

	public void setOsVer(String osVer) {
		this.osVer = osVer;
	}
}

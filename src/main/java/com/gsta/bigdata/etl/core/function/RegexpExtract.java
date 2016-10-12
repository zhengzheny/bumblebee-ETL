package com.gsta.bigdata.etl.core.function;

import java.util.Map;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringEscapeUtils;
import org.w3c.dom.Element;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.etl.core.ShellContext;

public class RegexpExtract extends AbstractFunction {
	private static final long serialVersionUID = 1L;
	@JsonProperty
 	private String inputField;
	@JsonProperty
	private String regexp;
	@JsonProperty
	private Pattern pattern;
	@JsonProperty
	private int idx = 1;

	public RegexpExtract() {
		super();
	}
	
	@Override
	protected void initAttrs(Element element) throws ParseException {
 		super.initAttrs(element);
 		
 		this.inputField = super.getAttr(Constants.ATTR_INPUT);
 		this.regexp = super.getAttr(Constants.ATTR_REGEXP);
 		String strIdx = super.getAttr(Constants.ATTR_INDEX);
 		if(strIdx != null){
 			try{
 				this.idx = Integer.parseInt(strIdx);
 			}catch(NumberFormatException e){
 				this.idx = 1;
 			}
 		}
 		this.pattern = Pattern.compile(StringEscapeUtils.unescapeJava(this.regexp));
	}

	@Override
	protected String onCalculate(Map<String, String> functionData,
			ShellContext context) throws ETLException {
		String value = functionData.get(inputField);
		if (null == value || "".equals(value)) {
			return null;
		}
		
		Matcher matcher = pattern.matcher(value);
		if (matcher.find()){
			MatchResult mr = matcher.toMatchResult();
			return mr.group(this.idx);
		}
		return null;
	}

	@Override
	protected Map<String, String> multiOutputOnCalculate(
			Map<String, String> functionData, ShellContext context)
			throws ETLException {
		// TODO Auto-generated method stub
		return null;
	}

}

package com.gsta.bigdata.etl.core.function.dpi;


public class SimpleRegularMatchRule extends UrlClassRule {
	private static final long serialVersionUID = -3808910630291982643L;

	public boolean isMatch(String url) {
		if (!super.isMatch(url))
			return false;

		if (!hasKeywords(url, false, false)) return false;
		
		try {
			if (rulePattern != null){
				return rulePattern.matcher(url).find();
			}
		} catch (Exception e) {
			matchErrorRules.put(rule, url);
		   	//context.getCounter(URL_CLASS_COUNTERS.URL_CLASS_COUNTER_MATCH_ERROR_TIMES).increment(1); 
		}
		
		return false;
	}
}

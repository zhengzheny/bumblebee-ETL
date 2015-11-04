package com.gsta.bigdata.etl.core.function.dpi;

public class FullMatchRule extends UrlClassRule {
	public boolean isMatch(String url) {
		if (!super.isMatch(url))
			return false;

		return url.equalsIgnoreCase(rule);
	}

}

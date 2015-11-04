package com.gsta.bigdata.etl.core.function.dpi;

public class RightMatchRule extends UrlClassRule {
	public boolean isMatch(String url) {
		if (!super.isMatch(url))
			return false;

		return url.endsWith(rule);
	}

}

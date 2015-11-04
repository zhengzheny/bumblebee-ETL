package com.gsta.bigdata.etl.core.function.dpi;

public class LeftMatchRule extends UrlClassRule {
	public boolean isMatch(String url) {
		if (!super.isMatch(url))
			return false;

		return url.startsWith(rule);
	}

}

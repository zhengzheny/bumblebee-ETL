package com.gsta.bigdata.etl.core.function.dpi;


public class AnyMatchRule extends UrlClassRule {
	public boolean isMatch(String url) {
		if (!super.isMatch(url))
			return false;

		return url.indexOf(rule) != -1;
	}
}

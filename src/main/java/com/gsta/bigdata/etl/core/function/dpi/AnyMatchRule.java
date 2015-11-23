package com.gsta.bigdata.etl.core.function.dpi;


public class AnyMatchRule extends UrlClassRule {
	private static final long serialVersionUID = 5563805564217218178L;

	public boolean isMatch(String url) {
		if (!super.isMatch(url))
			return false;

		return url.indexOf(rule) != -1;
	}
}

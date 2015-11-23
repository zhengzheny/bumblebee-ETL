package com.gsta.bigdata.etl.core.function.dpi;

public class FullMatchRule extends UrlClassRule {
	private static final long serialVersionUID = 7840196517019023450L;

	public boolean isMatch(String url) {
		if (!super.isMatch(url))
			return false;

		return url.equalsIgnoreCase(rule);
	}

}

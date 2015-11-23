package com.gsta.bigdata.etl.core.function.dpi;

public class RightMatchRule extends UrlClassRule {
	private static final long serialVersionUID = 6629400251241999737L;

	public boolean isMatch(String url) {
		if (!super.isMatch(url))
			return false;

		return url.endsWith(rule);
	}

}

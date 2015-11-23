package com.gsta.bigdata.etl.core.function.dpi;

public class LeftMatchRule extends UrlClassRule {
	private static final long serialVersionUID = 5234587257381509662L;

	public boolean isMatch(String url) {
		if (!super.isMatch(url))
			return false;

		return url.startsWith(rule);
	}

}

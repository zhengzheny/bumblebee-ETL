package com.gsta.bigdata.etl.core.function.dpi;

public class LeftRightMatchRule extends UrlClassRule {
	private static final long serialVersionUID = 2166820245239094985L;

	public boolean isMatch(String url) {
		if (!super.isMatch(url))
			return false;

		return hasKeywords(url, true, true);
	}

}

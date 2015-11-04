package com.gsta.bigdata.etl.core.function.dpi;

public class LeftRightMatchRule extends UrlClassRule {
	public boolean isMatch(String url) {
		if (!super.isMatch(url))
			return false;

		return hasKeywords(url, true, true);
	}

}

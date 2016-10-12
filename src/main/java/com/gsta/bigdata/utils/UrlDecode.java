package com.gsta.bigdata.utils;

import java.net.URLDecoder;

public class UrlDecode {
	private String url = null;
	private int times = 1;
	private String code = "UTF-8";

	public String decode(String urlStr, String srcCode, int count) {
		if (urlStr == null) {
			return null;
		}
		if (count <= 0) {
			return urlStr;
		}
		if (srcCode != null) {
			code = srcCode;
		}
		url = urlStr;
		times = count;
		for (int i = 0; i < times; i++) {
			url = decoder(url, code);
		}
		return url;
	}

	public String decode(String urlStr, String srcCode) {
		if (urlStr == null) {
			return null;
		}
		url = urlStr;
		code = srcCode;
		return decode(url, code, times);
	}

	public String decode(String urlStr, int count) {
		if (urlStr == null) {
			return null;
		}
		if (count <= 0) {
			return urlStr;
		}
		url = urlStr;
		times = count;

		return decode(url, code, times);
	}

	public String decode(String urlStr) {
		if (urlStr == null) {
			return null;
		}
		url = urlStr;
		return decode(url, code, times);
	}

	private String decoder(String urlStr, String code) {
		if (urlStr == null || code == null) {
			return null;
		}
		try {
			urlStr = URLDecoder.decode(urlStr, code);
		} catch (Exception e) {
			return null;
		}
		return urlStr;
	}

}

package com.gsta.bigdata.etl.core.function;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.w3c.dom.Element;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.etl.core.ShellContext;
import com.gsta.bigdata.etl.core.function.dpi.UrlInfo;

/**
 * 
 * @author shine
 *
 */
public class ParseURL extends AbstractFunction {
	private static final long serialVersionUID = -2978322391366886568L;
	
	@JsonProperty
	private String strInput;

	private static final Pattern ipCheckPat = Pattern.compile("([1-9]|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])(\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])){3}",Pattern.CASE_INSENSITIVE);

	public ParseURL() {
		super();
	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		super.initAttrs(element);

		this.strInput = super.getAttr(Constants.ATTR_INPUT);
		if (this.strInput == null || this.strInput.length() <= 0) {
			throw new ParseException(this.getClass().getSimpleName()
					+ " has no input attribute");
		}
	}

	@Override
	protected String onCalculate(Map<String, String> functionData,
			ShellContext context) throws ETLException {
		return null;
	}

	@Override
	protected Map<String, String> multiOutputOnCalculate(
			Map<String, String> functionData, ShellContext context)
			throws ETLException {
		Map<String, String> ret = new HashMap<String, String>();
		String value = functionData.get(strInput);
		if (value == null) {
			return null;
		}

		UrlInfo urlInfo = this.split(value);
		List<String> outputIds = super.getOutputIds();

		for (int i = 0; i < outputIds.size(); i++) {
			if (i == 0) {
				ret.put(outputIds.get(i), urlInfo.getDomain());
			} else if (i == 1) {
				ret.put(outputIds.get(i), urlInfo.getHost());
			} else if (i == 2) {
				ret.put(outputIds.get(i), urlInfo.getPath());
			} else if (i == 3) {
				ret.put(outputIds.get(i), urlInfo.getQuery());
			}
		}

		return ret;
	}

	/**
	 * parse URL
	 * 
	 * @param fullURL
	 * @return
	 */
	private UrlInfo split(String fullURL) {
		if (fullURL == null) {
			return null;
		}

		String url = fullURL;
		String urlHost = null;
		String urlPath = null;
		String urlQuery = null;

		UrlInfo urlInfo = new UrlInfo();
		urlInfo.setUrl(url);

		int pathIndex = fullURL.indexOf("/");
		if (pathIndex < 0) {
			urlInfo.setDomain(this.getDomainFromUrlHost(fullURL));
			urlInfo.setHost(fullURL);
			return urlInfo;
		} else if (pathIndex == 0) {
			urlPath = fullURL.substring(pathIndex + 1);
			int queryIndex = urlPath.indexOf("?");
			if (queryIndex > -1) {
				urlQuery = urlPath.substring(queryIndex + 1);
				urlPath = urlPath.substring(0, queryIndex);
			}

			urlInfo.setPath(urlPath);
			urlInfo.setQuery(urlQuery);

			return urlInfo;
		} else if (pathIndex == (fullURL.length() - 1)) {
			urlHost = fullURL.substring(0, pathIndex);
			urlInfo.setDomain(this.getDomainFromUrlHost(urlHost));
			urlInfo.setHost(urlHost);

			return urlInfo;
		} else {
			urlHost = fullURL.substring(0, pathIndex);
			urlPath = fullURL.substring(pathIndex + 1);
			int queryIndex = urlPath.indexOf("?");
			if (queryIndex > -1) {
				urlQuery = urlPath.substring(queryIndex + 1);
				urlPath = urlPath.substring(0, queryIndex);
			}

			urlInfo.setDomain(this.getDomainFromUrlHost(urlHost));
			urlInfo.setHost(urlHost);
			urlInfo.setPath(urlPath);
			urlInfo.setQuery(urlQuery);

			return urlInfo;
		}
	}

	/**
	 * extract a domain name according to the URL information
	 * 
	 * @param url
	 * @return
	 */
	private String getDomainFromUrlHost(String urlHost) {
		if (urlHost == null) {
			return null;
		}

		int indexDot = urlHost.lastIndexOf(".");
		if (indexDot < 0) {
			return urlHost;
		}

		if (this.isIp(urlHost)) {
			return urlHost;
		}

		String subStr = urlHost.substring(0, indexDot);
		indexDot = subStr.lastIndexOf(".", indexDot);
		if (indexDot < 0) {
			return urlHost;
		}

		String domain = urlHost.substring(indexDot + 1);
		if (domain.startsWith("com") || domain.startsWith("org")
				|| domain.startsWith("gov") || domain.startsWith("edu")
				|| domain.startsWith("net")) {
			subStr = subStr.substring(0, indexDot);
			indexDot = subStr.lastIndexOf(".", indexDot);
			if (indexDot < 0) {
				return domain;
			}
			domain = urlHost.substring(indexDot + 1);
		}
		return domain;
	}

	private boolean isIp(String ipAddress) {
		Matcher matcher = ipCheckPat.matcher(ipAddress);
		return matcher.matches();
	}

}

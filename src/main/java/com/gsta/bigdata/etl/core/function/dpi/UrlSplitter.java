package com.gsta.bigdata.etl.core.function.dpi;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author xiangy
 *
 */
public class UrlSplitter {
    private static final Pattern ipCheckPat = Pattern.compile("([1-9]|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])(\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])){3}", Pattern.CASE_INSENSITIVE);   

	UrlSplitter() {
	}

	public static UrlInfo split(String fullURL)
	{
		if (fullURL == null)
		{
			return null;
		}
		
		String url = fullURL;
		//String urlDomain = defaultValue;
		String urlHost = null;
		String urlPath = null;
		String urlQuery = null;

		UrlInfo urlInfo = new UrlInfo();
		urlInfo.setUrl(url);
		
		int pathIndex = fullURL.indexOf("/");
		if (pathIndex < 0) {
			urlInfo.setDomain(getDomainFromUrlHost(fullURL));
			urlInfo.setHost(fullURL);
			
			return urlInfo;
		} else if (pathIndex == 0) {
			urlPath = fullURL.substring(pathIndex + 1);
			int queryIndex = urlPath.indexOf("?");
			if (queryIndex > -1)
			{
				urlQuery = urlPath.substring(queryIndex + 1);
				urlPath = urlPath.substring(0, queryIndex);
			}

			urlInfo.setPath(urlPath);
			urlInfo.setQuery(urlQuery);
			return urlInfo;
		} else if (pathIndex == (fullURL.length() - 1)) {
			urlHost = fullURL.substring(0, pathIndex);
			urlInfo.setDomain(getDomainFromUrlHost(urlHost));
			urlInfo.setHost(urlHost);
			
			return urlInfo;
		}
		else
		{
			urlHost = fullURL.substring(0, pathIndex);
			urlPath = fullURL.substring(pathIndex + 1);
			int queryIndex = urlPath.indexOf("?");
			if (queryIndex > -1)
			{
				urlQuery = urlPath.substring(queryIndex + 1);
				urlPath = urlPath.substring(0, queryIndex);
			}
			
			urlInfo.setDomain(getDomainFromUrlHost(urlHost));
			urlInfo.setHost(urlHost);
			urlInfo.setPath(urlPath);
			urlInfo.setQuery(urlQuery);
			return urlInfo;
		}
	}

	
	private static boolean isIp(String ipAddress)  
	{  
	       Matcher matcher = ipCheckPat.matcher(ipAddress);   
	       return matcher.matches();   
	}  
	
	
	/**
	 * @param url
	 * @return
	 */
	private static String getDomainFromUrlHost(String urlHost)
	{
		if (urlHost == null)
		{
			return null;
		}

		int indexDot = urlHost.lastIndexOf(".");
		if (indexDot < 0)
		{
			return urlHost;
		}
		
		if (isIp(urlHost))
		{
			return urlHost;
		}
		
		
		String subStr = urlHost.substring(0, indexDot);
		indexDot = subStr.lastIndexOf(".", indexDot);
		if (indexDot < 0)
		{
			return urlHost;
		}
		
		String domain = urlHost.substring(indexDot+1);
		if (domain.startsWith("com") || domain.startsWith("org") || domain.startsWith("gov")
				|| domain.startsWith("edu") || domain.startsWith("net"))
		{
			subStr = subStr.substring(0, indexDot);
			indexDot = subStr.lastIndexOf(".", indexDot);
			if (indexDot < 0)
			{
				return domain;
			}
			domain = urlHost.substring(indexDot+1);
		}
		return domain;
	}
}

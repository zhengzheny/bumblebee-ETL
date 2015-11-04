package com.gsta.bigdata.etl.core.function.dpi;


/**
 * 
 * @author xiangy
 * 
 */
public class UrlInfo {
	// for 'movie.kankan.com/action/view.jsp?movieid=123456&refresh=1'
	// url   = movie.kankan.com/action/view.jsp?movieid=123456&refresh=1
	// domain= kankan.com
	// host = movie.kankan.com
	// path = /action/view.jsp
	// query = movieid=123456&refresh=1
	private String url; 
	private String domain;
	private String host; 
	private String path; 
	private String query;
	private String searchWords;

	private UrlClassRule urlRule; 

	/**
	 */
	public UrlInfo() {
	}

	/**
	 * @param domain
	 * @param host
	 * @param path
	 * @param query
	 * @param urlClassID
	 * @param urlRule
	 */
	public UrlInfo(String url, String domain, String host, String path, String query, UrlClassRule urlRule) {
		this.url = url;
		this.domain = domain;
		this.host = host;
		this.path = path;
		this.query = query;
		this.urlRule = urlRule;
	}

	public String getSearchWords() {
		return searchWords;
	}

	public void setSearchWords(String searchWords) {
		this.searchWords = searchWords;
	}
	
	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}
	
	public UrlClassRule getRule() {
		return urlRule;
	}

	public void setRule(UrlClassRule urlRule) {
		this.urlRule = urlRule;
	}

	public String getDomain() {
		return domain;
	}

	public void setDomain(String domain) {
		this.domain = domain;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}
}

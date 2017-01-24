package com.gsta.bigdata.etl.core.function;

import java.util.Map;

import org.w3c.dom.Element;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.etl.core.ShellContext;

public class MergeURL extends AbstractFunction {
	private static final long serialVersionUID = -5372473822138507658L;
	@JsonProperty
	private String url;
	@JsonProperty
	private String host;
	private final static String HTTP_HEADER = "http://";
	private final static String HTTPS_HEADER = "https://";

	public MergeURL() {
		super();
	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		super.initAttrs(element);
		
		this.url = super.getAttr(Constants.ATTR_URL);
 		this.host = super.getAttr(Constants.ATTR_HOST);
	}

	@Override
	protected String onCalculate(Map<String, String> functionData,
			ShellContext context) throws ETLException {
		if(functionData == null){
			return null;
		}
		
		String strURL = functionData.get(this.url);
		String strHost = functionData.get(this.host);
		if(strURL != null && strURL.startsWith("/")){
			return strHost+strURL;
		}else if(strURL != null && strURL.startsWith(HTTP_HEADER)){
			strURL = strURL.substring(HTTP_HEADER.length());
		}else if(strURL != null && strURL.startsWith(HTTPS_HEADER)){
			strURL = strURL.substring(HTTPS_HEADER.length());
		}
		
		return strURL;
	}

	@Override
	protected Map<String, String> multiOutputOnCalculate(
			Map<String, String> functionData, ShellContext context)
			throws ETLException {
		
		return null;
	}

}

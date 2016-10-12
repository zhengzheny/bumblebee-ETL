package com.gsta.bigdata.etl.core.function;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.w3c.dom.Element;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.etl.core.ShellContext;
import com.gsta.bigdata.utils.UrlDecode;

public class GetWeiBo extends AbstractFunction {
	private static final long serialVersionUID = -2542130414818539702L;
	@JsonProperty
	private String cookie;
	@JsonProperty
	private String host;
	private final UrlDecode urlDecode = new UrlDecode();
	@JsonProperty
	private List<String> outputIdList;

	public GetWeiBo() {
		super();
	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		super.initAttrs(element);

		this.cookie = super.getAttr(Constants.ATTR_COOKIE);
		this.host = super.getAttr(Constants.ATTR_HOST);
	}
	
	@Override
	public void init(Element element) throws ParseException {
		super.init(element);

		this.outputIdList = super.getOutputIds();
		int outSize = this.outputIdList.size();
		if (outSize != 3) {
			throw new ParseException(
					"GetWeiBo function must have 3 output value");
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
		if(functionData == null){
			return null;
		}
		
		Map<String, String> ret = new HashMap<String, String>();

		String vCookie = functionData.get(this.cookie);
		String vHost = functionData.get(this.host);
		String sub = this.getSub(vHost, vCookie);			
		ret.put(outputIdList.get(0), this.urlDecode.decode(sub));
		ret.put(outputIdList.get(1), this.getQueryField(sub, "name="));
		ret.put(outputIdList.get(2), this.getQueryField(sub, "nick="));
		
		return ret;
	}

	private String getSub(String vHost, String vCookie) {
		if (vHost != null && vCookie != null && vHost.contains("weibo.com")) {
			int idx = vCookie.indexOf("SUP=");
			if (idx > 0) {
				String sub = vCookie.substring(idx);
				if ("".equals(sub))  return null;
				// if SUP is in middle of cookie field,or SUP is the end of cookie field
				idx = sub.indexOf(";");
				if (idx > 0) {
					sub = sub.substring(0, idx);
				}
				sub = sub.substring(sub.indexOf("=") + 1);
				sub = this.urlDecode.decode(sub);

				return sub;
			}
		}

		return null;
	}
	
	private String getQueryField(String sub,String queryField){
		if(sub == null || "".equals(sub)) return null;
		
		int idx = sub.indexOf(queryField);
		if(idx > 0){
			String fieldValue = sub.substring(idx);
			if("".equals(fieldValue)) return null;
			
			idx = fieldValue.indexOf("&");
			fieldValue = fieldValue.substring(0, idx);
			fieldValue = fieldValue.substring(fieldValue.indexOf("=") +1);
			
			return this.urlDecode.decode(fieldValue);
		}
		
		return null;
	}
}

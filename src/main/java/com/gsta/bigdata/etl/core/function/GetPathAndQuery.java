package com.gsta.bigdata.etl.core.function;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.w3c.dom.Element;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.etl.core.ShellContext;

public class GetPathAndQuery extends AbstractFunction {
	private static final long serialVersionUID = -2542130414818539702L;
	@JsonProperty
	private String url;
	@JsonProperty
	private String host;
	@JsonProperty
	private List<String> outputIdList;
	
	public GetPathAndQuery() {
		super();
	}
	
	@Override
	protected void initAttrs(Element element) throws ParseException {
 		super.initAttrs(element);
 		
 		this.url = super.getAttr(Constants.ATTR_URL);
 		this.host = super.getAttr(Constants.ATTR_HOST);
	}
	
	@Override
	public void init(Element element) throws ParseException {
		super.init(element);

		this.outputIdList = super.getOutputIds();
		int outSize = this.outputIdList.size();
		if (outSize != 2) {
			throw new ParseException(
					"GetPathAndQuery function must have 3 output value");
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

		String vURL = functionData.get(this.url);
		String vHost = functionData.get(this.host);
		String str = "http://" + vHost + vURL;
		try {
			URL url = new URL(str);
			ret.put(outputIdList.get(0), url.getPath());
			ret.put(outputIdList.get(1), url.getQuery());
			
		} catch (MalformedURLException e) {}			
		
		return ret;
	}
}

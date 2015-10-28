package com.gsta.bigdata.etl.core.function;

import java.util.HashMap;
import java.util.Map;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.etl.core.ShellContext;

public class ParseURL extends AbstractFunction {
	private static final long serialVersionUID = -2978322391366886568L;
	private final static String INPUT_URL = "url";
	private final static String URLDOMAIN = "urldomain";
	private final static String URLHOST   = "urlhost";  
	private final static String URLPATH   = "urlpath";  
	private final static String URLQUERY  = "urlquery"; 
	@JsonProperty
	private String strInput;
	
	//private Logger logger = LoggerFactory.getLogger(getClass());

	public ParseURL() {
		super();
	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		super.initAttrs(element);
		
		this.strInput = super.getAttr(Constants.ATTR_INPUT);
		if(this.strInput == null || this.strInput.length() <= 0){
			throw new ParseException(this.getClass().getSimpleName() + " has no input attribute");
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
		if(this.strInput.equals(INPUT_URL)){
			ret.put(URLDOMAIN, "www.baidu.com");
			ret.put(URLHOST, "10.17.36.16");
			ret.put(URLPATH, "/news/sports");
			ret.put(URLQUERY, "?page=1&id=2");
		}
		
		return ret;
	}

}

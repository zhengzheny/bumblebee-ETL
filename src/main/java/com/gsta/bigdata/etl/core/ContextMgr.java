package com.gsta.bigdata.etl.core;

import java.io.Serializable;

/**
 * 
 * @author tianxq
 * 
 */
public class ContextMgr implements Serializable{
	private static final long serialVersionUID = -7999136276966399765L;

	/**
	 * first get variable from shell command line
	 * second get variable from .properties file,
	 * @param value
	 * @return
	 */
	public static String getValue(String value){
		if(value == null){
			return null;
		}
		
		String ret = null;
		//update ${variable} variable from shell command
		if(value.indexOf(Constants.CONTEXT_PREFIX) != -1){
			int beginPos = value.indexOf(Constants.CONTEXT_PREFIX);
			int endPos = value.indexOf(Constants.CONTEXT_POSTFIX);
			String key = value.substring(beginPos + Constants.CONTEXT_PREFIX.length(), endPos);
			ret = ShellContext.getInstance().getValue(key);
			if(!ret.equals(key)){
				return ret;
			}
		}
		
		//update ${variable} variable from .properties file
		ret = ContextProperty.getInstance().getValue(value);
		
		//update context ${YYYYMM} from shell command line
		//if have multiple variable ,replace all
		String attrValue = ret;
		while( attrValue.indexOf(Constants.CONTEXT_PREFIX)  != -1){
			int beginPos = attrValue.indexOf(Constants.CONTEXT_PREFIX);
			int endPos = attrValue.indexOf(Constants.CONTEXT_POSTFIX);
			
			String var = attrValue.substring(beginPos,endPos + Constants.CONTEXT_POSTFIX.length());
			String key = attrValue.substring(beginPos + Constants.CONTEXT_PREFIX.length(), endPos);
			
			String shellContext = ShellContext.getInstance().getValue(key);
			ret = ret.replace(var, shellContext);
			
			attrValue = attrValue.substring(endPos + Constants.CONTEXT_POSTFIX.length());
		}
		
		return ret;
	}
	
}

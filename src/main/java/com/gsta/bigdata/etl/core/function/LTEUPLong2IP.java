package com.gsta.bigdata.etl.core.function;

import java.util.Map;

import org.w3c.dom.Element;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.etl.core.ShellContext;

public class LTEUPLong2IP extends AbstractFunction {
	private static final long serialVersionUID = -1736024035415593085L;
	@JsonProperty
	private String inputField;

	public LTEUPLong2IP() {
		super();
	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		super.initAttrs(element);

		this.inputField = super.getAttr(Constants.ATTR_INPUT);
	}

	@Override
	protected String onCalculate(Map<String, String> functionData,
			ShellContext context) throws ETLException {
		String data = functionData.get(this.inputField);
		if(data == null || "".equals(data)){
			return null;
		}
		
		try {
			long lip = Long.parseLong(data);
			if(lip ==0 ){
				return "0.0.0.0";
			}else if(lip > 0){
				StringBuffer sb = new StringBuffer();
				long ip =Math.round(Math.floor((lip%(128*256*256*256l))/(256*256*256l)));
				sb.append(ip).append(".");
				ip = Math.round(Math.floor((lip%(256*256*256l))/(256*256l)));
				sb.append(ip).append(".");
				ip =  Math.round(Math.floor((lip%(256*256l))/256)); 
				sb.append(ip).append(".");
				ip =  Math.round(Math.floor((lip%256)/1));
				sb.append(ip);
				return sb.toString();
			}else if(lip <0){
				StringBuffer sb = new StringBuffer();
				long ip = Math.round(Math.floor(((lip+128*256*256*256l)%(128*256*256*256l))/(256*256*256l)+128));
				sb.append(ip).append(".");
				ip = Math.round(Math.floor(((lip+128*256*256*256l)%(256*256*256l))/(256*256)));
				sb.append(ip).append(".");
				ip=Math.round(Math.floor(((lip+128*256*256*256l)%(256*256))/256));
				sb.append(ip).append(".");
				ip = Math.round(Math.floor(((lip+128*256*256*256l)%256)/1));
				sb.append(ip);
				return sb.toString();
			}
		} catch (NumberFormatException e) {
			throw new ETLException(e);
		}
		
		return null;
	}

	@Override
	protected Map<String, String> multiOutputOnCalculate(
			Map<String, String> functionData, ShellContext context)
			throws ETLException {
		return null;
	}
}

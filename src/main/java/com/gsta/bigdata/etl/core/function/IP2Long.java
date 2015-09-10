package com.gsta.bigdata.etl.core.function;

import java.util.Map;

import org.w3c.dom.Element;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ShellContext;
import com.gsta.bigdata.etl.core.ParseException;


/**
 * function demo
 * in this function,don't change input ip value.
 * you can set new output field.
 * if you'd like to change ip value,reset in onCalculate function
 * @author tianxq
 *
 */
public class IP2Long extends AbstractFunction {
	@JsonProperty
	private String inputField;
	
	public IP2Long() {
		super();
	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		super.initAttrs(element);		
		inputField = super.getAttr(Constants.ATTR_INPUT);
	}

	@Override
	public String onCalculate(Map<String, String> functionData,
			ShellContext context) throws ETLException {
		String ip = functionData.get(inputField);		
		return String.valueOf(ip2Long(ip));
	}

	private Long ip2Long(String ip) {
		try {
			String[] ips = ip.split("[.]");
			
			long num = 16777216L * Long.parseLong(ips[0]) + 65536L
					* Long.parseLong(ips[1]) + 256 * Long.parseLong(ips[2])
					+ Long.parseLong(ips[3]);
			
			return num;
		} catch (Exception ex) {
			return 0L;
		}
	}
	
	@Override
	public Map<String, String> multiOutputOnCalculate(
			Map<String, String> functionData, ShellContext context)
			throws ETLException {
		return null;
	}
}

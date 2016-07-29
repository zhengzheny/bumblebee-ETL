package com.gsta.bigdata.etl.core.function;

import java.util.Map;

import org.w3c.dom.Element;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ShellContext;
import com.gsta.bigdata.etl.core.ParseException;
/**
 * 
 * @author tianxq
 *
 */
public class Long2IP extends AbstractFunction {
	private static final long serialVersionUID = -4133280405610886749L;
	@JsonProperty
	private String inputField;
	
	public Long2IP() {
		super();
	}
	
	@Override
	protected void initAttrs(Element element) throws ParseException {
		super.initAttrs(element);
		
		inputField = super.getAttr(Constants.ATTR_INPUT);
	}

	@Override
	protected String onCalculate(Map<String, String> functionData,
			ShellContext context) throws ETLException {
		String ip = functionData.get(this.inputField);

		return long2IP(ip);
	}

	private String long2IP(String longIp) throws ETLException{
		long mask[] = { 0x000000FF, 0x0000FF00, 0x00FF0000, 0xFF000000 };
		long num = 0;

		StringBuffer ip = new StringBuffer();
		try {
			Long ipLong = Long.parseLong(longIp);
			if(ipLong < 0){
				//-593129911  220.165.142.73
				ipLong = ipLong & 0x0FFFFFFFFL;
			}

			ip.setLength(0);
			for (int i = 0; i < 4; i++) {
				num = (ipLong & mask[i]) >> (i * 8);
				if (i > 0)
					ip.insert(0, ".");
				ip.insert(0, Long.toString(num, 10));
			}
			
			return ip.toString();
		} catch (Exception ex) {
			throw new ETLException(ETLException.LONG_TO_IP,"transform long:" + longIp
					+ " to ip occurs error.");
		}
	}
	
	@Override
	protected Map<String, String> multiOutputOnCalculate(
			Map<String, String> functionData, ShellContext context)
			throws ETLException {
		return null;
	}
}

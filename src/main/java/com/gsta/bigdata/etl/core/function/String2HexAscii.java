package com.gsta.bigdata.etl.core.function;

import java.util.Map;

import org.w3c.dom.Element;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ShellContext;
import com.gsta.bigdata.etl.core.ParseException;


/**
 * transform string to Hex Ascii
 * @author shine
 *
 */
public class String2HexAscii extends AbstractFunction {
	private static final long serialVersionUID = 3119221817442717149L;
	@JsonProperty
	private String inputField;
	
	public String2HexAscii() {
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
		String value = functionData.get(inputField);		
		return string2HexAscii(value);
	}

	private String string2HexAscii(String str) {
		if (null == str || "".equals(str)) {
			return "";
		}
		
		StringBuffer sb = new StringBuffer();
		try {
			byte[] b = str.getBytes();
			//sb.append("Ox");
			sb.append("0x");
			for (int i = 0; i < b.length; i++) {
				sb.append(Integer.toString((b[i] & 0xff) + 0x100, 16)
						.substring(1));
			}
		} catch (Exception e) {
			new ETLException(ETLException.STRING_TO_ASCII,"string:" + str + " transform to ASCII error");
		}
			
		return sb.toString();
	}
	
	@Override
	protected Map<String, String> multiOutputOnCalculate(
			Map<String, String> functionData, ShellContext context)
			throws ETLException {
		return null;
	}
}

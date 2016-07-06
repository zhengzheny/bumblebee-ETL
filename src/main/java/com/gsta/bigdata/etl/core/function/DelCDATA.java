package com.gsta.bigdata.etl.core.function;

import java.util.Map;

import org.w3c.dom.Element;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.etl.core.ShellContext;

/**
 * delete CDATA from field,ex:<![CDATA[YJ-GX-HLDZPQXJ]]>,return YJ-GX-HLDZPQXJ
 * @author tianxq
 *
 */
public class DelCDATA extends AbstractFunction {
	private static final long serialVersionUID = 7684108245478573657L;
	@JsonProperty
 	private String inputField;
	private final static String BEGINER = "<![CDATA[";
	private final static String ENDER = "]]>";
	private final static int BEGIN_INDEX = BEGINER.length();
	
	public DelCDATA() {
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
		String value = functionData.get(this.inputField);
		if(value != null && value.startsWith(BEGINER) && value.endsWith(ENDER)){
			int pos = value.indexOf(ENDER);
			return value.substring(BEGIN_INDEX, pos);
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

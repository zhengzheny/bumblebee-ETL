package com.gsta.bigdata.etl.core.function;

import java.util.Map;

import org.w3c.dom.Element;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ShellContext;
import com.gsta.bigdata.etl.core.ParseException;

/**
 * delete wrapper
 * 
 * @author Shine
 * 
 */
public class DelWrapper extends AbstractFunction {
	@JsonProperty
	private String wrapper;
 	@JsonProperty
 	private String inputField;
	private static final String ATTR_WRAPPER = "wrapper";

	public DelWrapper() {
		super();
	}
 	
 
	@Override
	protected void initAttrs(Element element) throws ParseException {
 		super.initAttrs(element);
 		
		this.wrapper = super.getAttr(ATTR_WRAPPER);
 		this.inputField = super.getAttr(Constants.ATTR_INPUT);
	}

	@Override
	protected String onCalculate(Map<String, String> functionData,
			ShellContext context) throws ETLException {
 		String value = functionData.get(inputField);
 		return delWrapper(value);
	}

	/**
	 * @param value
	 * @param wrap
	 * @return
	 */
	private String delWrapper(String value) {
		if (null == value || "".equals(value)) {
			return "";
		}
		return value.replace(this.wrapper, "");
	}


	@Override
	protected Map<String, String> multiOutputOnCalculate(
			Map<String, String> functionData, ShellContext context)
			throws ETLException {
		return null;
	}
}
package com.gsta.bigdata.etl.core.function;

import java.util.Map;

import org.w3c.dom.Element;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ShellContext;
import com.gsta.bigdata.etl.core.ParseException;

/**
 * delete tab
 * 
 * @author Shine
 * 
 */
public class DelTab extends AbstractFunction {
	private static final long serialVersionUID = 6226407600062549056L;
 	@JsonProperty
 	private String inputField;

	public DelTab() {
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
 		String value = functionData.get(inputField);
 		return delTab(value);
	}

	/**
	 * @param value
	 * @return
	 */
	private String delTab(String value) {
		if (null == value || "".equals(value)) {
			return "";
		}
		
		return value.replace("\t", "");
	}


	@Override
	protected Map<String, String> multiOutputOnCalculate(
			Map<String, String> functionData, ShellContext context)
			throws ETLException {
		return null;
	}
}
package com.gsta.bigdata.etl.core.function;

import java.util.Map;

import org.w3c.dom.Element;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.etl.core.ShellContext;
import com.gsta.bigdata.utils.MD5Util;

public class MSISDNMd5 extends AbstractFunction {
	private static final long serialVersionUID = -6436890928171467326L;
	@JsonProperty
	private String inputField;
	
	public MSISDNMd5() {
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
		
		if (data != null) {
			if (data.length() == 11 && data.substring(1, 2) != "86") {
				data = "86" + data.trim();
			}
			//return Hashing.md5().hashString(data.trim()).toString();
			return MD5Util.md5(data.trim());
		}
		
		return null;
	}

	@Override
	protected Map<String, String> multiOutputOnCalculate(
			Map<String, String> functionData, ShellContext context)
			throws ETLException {
		// TODO Auto-generated method stub
		return null;
	}

}

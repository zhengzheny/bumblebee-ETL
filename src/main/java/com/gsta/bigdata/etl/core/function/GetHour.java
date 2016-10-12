package com.gsta.bigdata.etl.core.function;

import java.util.Date;
import java.util.Map;

import org.w3c.dom.Element;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.etl.core.ShellContext;

public class GetHour extends AbstractFunction {
	private static final long serialVersionUID = -2542130414818539702L;
	@JsonProperty
	private String timestamp;
	
	public GetHour() {
		super();
	}
	
	@Override
	protected void initAttrs(Element element) throws ParseException {
 		super.initAttrs(element);
 		
 		this.timestamp = super.getAttr(Constants.ATTR_TIMESTAMP);
	}

	@SuppressWarnings("deprecation")
	@Override
	protected String onCalculate(Map<String, String> functionData,
			ShellContext context) throws ETLException {
		if(functionData == null){
			return null;
		}
		
		String vtimestamp = functionData.get(this.timestamp);
		if (vtimestamp != null && !"".equals(vtimestamp)) {
			int idx = vtimestamp.indexOf(".");
			if (idx > 0) {
				vtimestamp = vtimestamp.substring(0,idx);
			}

			try {
				Long t = Long.parseLong(vtimestamp) * 1000;
				Date d = new Date(t);
				return String.valueOf(d.getHours());
			} catch (Exception e) {}
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

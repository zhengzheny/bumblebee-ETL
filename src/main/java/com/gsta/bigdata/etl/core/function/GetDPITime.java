package com.gsta.bigdata.etl.core.function;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.w3c.dom.Element;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.etl.core.ShellContext;

public class GetDPITime extends AbstractFunction {
	private static final long serialVersionUID = -2542130414818539702L;
	@JsonProperty
	private String timestamp;
	@JsonProperty
	private List<String> outputIdList;
	@JsonProperty
	private String formatter = "yyyyMMddHHmmss";
	private SimpleDateFormat  sdf ;
	
	public GetDPITime() {
		super();
	}
	
	@Override
	protected void initAttrs(Element element) throws ParseException {
 		super.initAttrs(element);
 		
 		this.timestamp = super.getAttr(Constants.ATTR_TIMESTAMP);
 		this.formatter = super.getAttr(Constants.ATTR_FORMATTER);
 		this.sdf = new SimpleDateFormat (this.formatter);
	}

	@Override
	public void init(Element element) throws ParseException {
		super.init(element);

		this.outputIdList = super.getOutputIds();
		int outSize = this.outputIdList.size();
		if (outSize != 3) {
			throw new ParseException(
					"GetPathAndQuery function must have 3 output value");
		}
	}
	
	@Override
	protected String onCalculate(Map<String, String> functionData,
			ShellContext context) throws ETLException {
		return null;
	}

	@SuppressWarnings("deprecation")
	@Override
	protected Map<String, String> multiOutputOnCalculate(
			Map<String, String> functionData, ShellContext context)
			throws ETLException {
		if(functionData == null){
			return null;
		}
		
		Map<String, String> ret = new HashMap<String, String>();
		
		String vtimestamp = functionData.get(this.timestamp);
		String microseconds = "";
		if (vtimestamp != null && !"".equals(vtimestamp)) {
			int idx = vtimestamp.indexOf(".");
			if (idx > 0) {
				microseconds = vtimestamp.substring(idx, vtimestamp.length());
				vtimestamp = vtimestamp.substring(0,idx);
			}

			try {
				Date d = new Date(Long.parseLong(vtimestamp) * 1000);
				ret.put(outputIdList.get(0), String.valueOf(d.getHours()));
				String tt = this.sdf.format(d);
				ret.put(outputIdList.get(1), tt + microseconds);
				ret.put(outputIdList.get(2), tt.substring(0, 8));
			} catch (Exception e) {}
		}
		
		return ret;
	}
}

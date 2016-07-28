package com.gsta.bigdata.etl.core.function;

import java.util.Map;

import org.w3c.dom.Element;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.etl.core.ShellContext;

public class LTEUPFlag extends AbstractFunction {
	private static final long serialVersionUID = 6897677295119057085L;
	private static final String FLAG_FIELD = "flagField";

	@JsonProperty
	private String[] fields;

	public LTEUPFlag() {
		super();
	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		super.initAttrs(element);
		String inputField = super.getAttr(FLAG_FIELD);
		if (inputField != null) {
			this.fields = inputField.split(",", -1);
			if (this.fields == null || this.fields.length < 8) {
				throw new ParseException(
						"LTEUPFlag inputField must have 8 field.");
			}
		}
	}

	@Override
	protected String onCalculate(Map<String, String> functionData,
			ShellContext context) throws ETLException {
		if (functionData == null) {
			return null;
		}
		
		/**
		 * ((case when length(IMSI)<>15 or substr(IMSI,1,3)<>'460'then 1 else 0 end)
|(case when length(MSISDN)<>11 and length(MSISDN)<>13 then 2 else 0 end)
|(case when CGI=0 or CGI is NULL then 4 else 0 end)
|(case when ts_end=0 or length(ts_end)=0 then 8 else 0 end)
|(case when ts_first_dl=0 or length(ts_first_dl)=0 then 16 else 0 end)
|(case when ts_first_ul=0 or length(ts_first_ul)=0 then 32 else 0 end)
|(case when ts_last_dl=0 or length(ts_last_dl)=0 then 64 else 0 end)
|(case when service_id=0 or service_id is NULL then 128 else 0 end)) as FLAG    
		 */
		int flagIMSI = 0, flagMSISDN = 0, flagCGI = 0, flagTs_end = 0;
		int flagTs_first_dl = 0, flagTs_first_ul = 0, flagTs_last_dl = 0, flagService_id = 0;

		String IMSI = functionData.get(this.fields[0]);
		if (IMSI != null && (IMSI.length() != 15 || !"460".equals(IMSI.substring(0, 3)))) {
			flagIMSI = 1;
		}

		String MSISDN = functionData.get(this.fields[1]);
		if (MSISDN != null && MSISDN.length() != 11 && MSISDN.length() != 13) {
			flagMSISDN = 2;
		}

		String CGI = functionData.get(this.fields[2]);
		if (CGI == null || "0".equals(CGI)) {
			flagCGI = 4;
		}
		
		String ts_end = functionData.get(this.fields[3]);
		if(ts_end != null &&("0".equals(ts_end) || ts_end.length() == 0)){
			flagTs_end = 8;
		}
		
		String ts_first_dl = functionData.get(this.fields[4]);
		if(ts_first_dl != null &&("0".equals(ts_first_dl) || ts_first_dl.length() == 0)){
			flagTs_first_dl = 16;
		} 
		
		String ts_first_ul = functionData.get(this.fields[5]);
		if(ts_first_ul != null &&("0".equals(ts_first_ul) || ts_first_ul.length() == 0)){
			flagTs_first_ul = 32;
		} 
		
		String ts_last_dl = functionData.get(this.fields[6]);
		if(ts_last_dl != null &&("0".equals(ts_last_dl) || ts_last_dl.length() == 0)){
			flagTs_last_dl = 64;
		} 
		
		String service_id = functionData.get(this.fields[7]);
		if(service_id == null || "0".equals(service_id)){
			flagService_id = 128;
		}

		int flag = flagIMSI | flagMSISDN | flagCGI | flagTs_end
				| flagTs_first_dl | flagTs_first_ul 
				| flagTs_last_dl | flagService_id;
		
		return String.valueOf(flag);
	}

	@Override
	protected Map<String, String> multiOutputOnCalculate(
			Map<String, String> functionData, ShellContext context)
			throws ETLException {
		return null;
	}

	public void setFields(String[] fields) {
		this.fields = fields;
	}

	public static void main(String[] args){
		String[] fields = {"IMSI","MSISDN","CGI","ts_end","ts_first_dl","ts_first_ul","ts_last_dl","service_id"};
		
		Map<String, String> data = new java.util.HashMap<String, String>();
		
		data.put("IMSI", "460110402779374");
		data.put("MSISDN", "18002248964");
		data.put("CGI", "122776577");
		data.put("ts_end", "2016-07-25 14:21:13.3530990");
		data.put("ts_first_dl", "2016-07-25 14:18:44.4864800");
		data.put("ts_first_ul", "2016-07-25 14:18:44.4554840");
		data.put("ts_last_dl", "2016-07-25 14:19:17.6076940");
		data.put("service_id", "0");
		
		LTEUPFlag flag = new LTEUPFlag();
		flag.setFields(fields);
		System.out.println(flag.onCalculate(data, null));
	
	}
}

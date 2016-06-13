package com.gsta.bigdata.etl.core.function;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import org.w3c.dom.Element;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ShellContext;

/**
 * judge workday or weekend
 * 
 * @author Shine
 * 
 */
public class IsHoliday extends AbstractFunction {
	private static final long serialVersionUID = -2173279221780483668L;
	@JsonProperty
	private String inputField;
	@JsonProperty
	private String formatter;

	private static final String ATTR_FORMATTER = "formatter";	
	private static final String RESULT_WORKDAY = "workday";
	private static final String RESULT_WEEKEND = "weekend";	
	private static final String DEFAULT_FORMATTER = "yyyy-MM-dd HH:mm:ss.SSS";

	public IsHoliday() {
		super();
	}

	@Override
	protected void initAttrs(Element element)
			throws com.gsta.bigdata.etl.core.ParseException {
		super.initAttrs(element);

		this.formatter = super.getAttr(ATTR_FORMATTER);
		if (null == this.formatter || "".equals(this.formatter)) {
			this.formatter = DEFAULT_FORMATTER;
		}

		this.inputField = super.getAttr(Constants.ATTR_INPUT);
	}

	@Override
	protected String onCalculate(Map<String, String> functionData,
			ShellContext context) throws ETLException {
		String date = functionData.get(inputField);
		return isHoliday(date);
	}

	/**
	 * @param strDate
	 * @return
	 */
	private String isHoliday(String strDate) {
		if(strDate == null || "".equals(strDate)){
			return null;
		}
			
		String result = null;
		try {
			SimpleDateFormat sdf = new SimpleDateFormat(this.formatter);
			
			Date date = sdf.parse(strDate);
			Calendar cal = Calendar.getInstance();
			cal.setTime(date);
			int n = cal.get(Calendar.DAY_OF_WEEK);
			if (n != 1 && n != 7) {
				result = RESULT_WORKDAY;
			} else {
				result = RESULT_WEEKEND;
			}
		} catch (ParseException e) {
			throw new ETLException(ETLException.DATE_FORMAT,"date:" + strDate + "format error");
		}
		
		return result;
	}
	
	@Override
	protected Map<String, String> multiOutputOnCalculate(
			Map<String, String> functionData, ShellContext context)
			throws ETLException {
		return null;
	}
}

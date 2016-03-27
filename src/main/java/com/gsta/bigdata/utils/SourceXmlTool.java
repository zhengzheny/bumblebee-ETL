package com.gsta.bigdata.utils;

import org.apache.commons.lang.StringUtils;

import com.gsta.bigdata.etl.ETLException;

public class SourceXmlTool {
	/**
	 * get attribute value from line
	 * @param str eg:<fileHeader startTime="2016-03-13T06:30:00.000" endTime="2016-03-13T06:45:00.000"/>
	 * @param attrName:startTime
	 * @return 2016-03-13T06:30:00.000
	 */
	public static String getAttrValue(String str, String attrName) {
		if (str == null || "".equals(str)) {
			return null;
		}

		if (attrName == null || "".equals(str)) {
			return null;
		}

		int index = str.indexOf(attrName);
		if (index != -1) {
			int begin = index + attrName.length() + 2;  //2 means ="
			String temp = str.substring(begin);
			int end = temp.indexOf("\"");
			return temp.substring(0, end);
		}

		return null;
	}

	/**
	 * 
	 * @param str <smr>MR.LteTddNcPci MR.UtraCarrierRSSI</smr>
	 * @param tagName smr
	 * @return MR.LteTddNcPci MR.UtraCarrierRSSI
	 * @throws ETLException
	 */
	public static String getTagValue(String str, String tagName) throws ETLException {
		if (StringUtils.isBlank(str) || StringUtils.isBlank(tagName)) {
			return null;
		}

		String ret = null;
		try {
			int index = str.indexOf(tagName);
			if (index != -1) {
				int begin = index + tagName.length() + 1;
				int end = str.lastIndexOf("<");
				ret = str.substring(begin, end);
			}
		} catch (Exception e) {
			throw new ETLException(ETLException.GET_TAG_VALUE_ERROR,"get tag value error,tag=" + str);
		}

		return ret;
	}
}

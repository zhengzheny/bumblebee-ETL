package com.gsta.bigdata.etl.flume;

import java.util.HashMap;
import java.util.Map;

import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.source.MroHuaWei;

public class FieldsHeader {
	private int fileCount;
	private String strHeaderFields;
	private String[] headerFields;
	private final static String MRO_TIMESTAMP = "mro-TimeStamp";
	private final static String MRO_ENODEID = "mro-ENODEID";

	public FieldsHeader(String strHeaderFields, int fileCount) {
		this.strHeaderFields = strHeaderFields;
		this.fileCount = fileCount;

		if (this.strHeaderFields != null) {
			this.headerFields = this.strHeaderFields.split(",", -1);
		}
	}

	public Map<String, String> parseHeaders(ETLData etlData) {
		Map<String, String> ret = new HashMap<String, String>();

		if (etlData == null || this.headerFields == null) {
			return ret;
		}

		for (String field : this.headerFields) {
			if (field.equals(MRO_TIMESTAMP)) {
				ret.put(MRO_TIMESTAMP, 
						this.getTimeStamp(etlData.getValue(MroHuaWei.FIELD_TIMESTAMP)));
			} else if (field.equals(MRO_ENODEID)) {
				ret.put(MRO_ENODEID, 
						this.getEnodeId(etlData.getValue(MroHuaWei.FIELD_ENODEBID)));
			} else {
				ret.put(field, etlData.getValue(field));
			}
		}

		return ret;
	}

	/**
	 * 
	 * @param timeStamp
	 *            "2016-06-04 00:00:01.893"
	 * @return 2016060400
	 */
	private String getTimeStamp(String timeStamp) {
		String ret = "unknown";
		if (timeStamp != null && timeStamp.contains(":")) {
			ret = timeStamp.substring(0, timeStamp.indexOf(":"))
					.replace('-', ' ').replace(" ", "");
		}

		return ret;
	}

	/**
	 * 
	 * @param eNodeId
	 * @return enodeId % fileCount
	 */
	private String getEnodeId(String eNodeId) {
		String ret = "unknown";
		if (eNodeId == null) {
			return ret;
		}

		try {
			int id = Integer.parseInt(eNodeId);
			return String.valueOf(id % this.fileCount);
		} catch (NumberFormatException e) {
			return ret;
		}
	}
}

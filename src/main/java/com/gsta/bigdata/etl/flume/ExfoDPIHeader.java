package com.gsta.bigdata.etl.flume;

import java.util.HashMap;
import java.util.Map;

public class ExfoDPIHeader implements IFileNameHeader {
	/**
	 * fileName:ETL_LTEUP_HttpWap_201607251421.a.731.1171.csv.done
	 */
	@Override
	public Map<String, String> parseHeaders(String fileName) {
		Map<String, String> ret = new HashMap<String, String>();
		
		int pos = fileName.indexOf(".");
		if(fileName != null && pos != -1){
			String str = fileName.substring(0,pos);
			String[] fields = str.split("_",-1);
			if(fields != null && fields.length == 4){
				String part1 = fields[1];
				String part2 = fields[2];
				ret.put("p1", part1.toUpperCase());
				ret.put("p2", part2.toUpperCase());
				
				String time = fields[3];
				if(time.length() >= 10){
					String year = time.substring(0,4);
					String month = time.substring(4,6);
					String day = time.substring(6,8);
					String hour = time.substring(8,10);
					ret.put("year", year);
					ret.put("month", month);
					ret.put("day", day);
					ret.put("hour", hour);
				}
			}
		}
			
		return ret;
	}
}

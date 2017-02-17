package com.gsta.bigdata.etl.flume.sources;

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.interceptor.Interceptor;

public class HWKPIInterceptor extends AbstractKPIInterceptor {
	public HWKPIInterceptor(String configFilePath, String[] types) {
		super(configFilePath, types);
	}

	@Override
	protected String getConfigFileByType(String type) {
		return super.configFilePath + "ETL_KPI_HW_" + type + ".xml";
	}

	@Override
	protected String getTypeFromSource(String fileName) {
		//pmresult_1526726660_15_201612220300_201612220315.xml
		//return 660_15
		if(fileName != null){
			String[] part = fileName.split("_",-1);
			if(part.length >= 3){
				String p1 = part[1];
				if(p1.length() > 3){
					p1 = p1.substring(p1.length()-3);
				}
				return p1 + "_" + part[2];
			}
		}
		return null;
	}

	@Override
	protected Map<String, String> parseHeadersBySource(String fileName) {
		//pmresult_1526726660_15_201612220300_201612220315.xml
		//HEADER_KPI_DATE=20161222
		//HEADER_KPI_HOUR=0300
		boolean flag = false;
		Map<String, String> headers = new HashMap<String, String>();
		if(fileName != null){
			String[] part = fileName.split("_",-1);
			if(part.length >= 4){
				String p = part[3];
				if(p.length() >= 12){
					String d = p.substring(0, 8);
					String h = p.substring(8);
					headers.put(HEADER_KPI_DATE, d);
					headers.put(HEADER_KPI_HOUR, h);
					flag = true;
				}
			}
		}
		
		if(!flag){
			headers.put(HEADER_KPI_DATE, HEADER_DEFAULT_VALUE);
			headers.put(HEADER_KPI_HOUR, HEADER_DEFAULT_VALUE);
		}
		return headers;
	}
	
	public static class Builder extends AbstractKPIBuilder {
		@Override
		public Interceptor build() {
			return new HWKPIInterceptor(super.configFilePath, super.types);
		}
	}

	public static void main(String[] args) {
		String fileName = "pmresult_1526726660_15_201612220300_201612220315.xml";
		
		HWKPIInterceptor kpi = new HWKPIInterceptor(null,null);
		System.out.println(kpi.getTypeFromSource(fileName));
		System.out.println(kpi.parseHeadersBySource(fileName));
	}

}

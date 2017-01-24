package com.gsta.bigdata.etl.flume;

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.interceptor.Interceptor;

public class ZTEKPIInterceptor extends AbstractKPIInterceptor {
	public ZTEKPIInterceptor(String configFilePath, String[] types) {
		super(configFilePath, types);
	}

	@Override
	protected String getConfigFileByType(String type){
		return super.configFilePath + "ETL_KPI_ZTE_" + type + ".xml";
	}
	
	@Override
	protected String getTypeFromSource(String fileName) {
		//PM201612220330+0800_20161222.0300+0800-0315+0800_CELLPOWER.xml
		//return CELLPOWER
		if (fileName != null) {
			int pos = fileName.lastIndexOf("_");
			if (pos > 0) {
				String key = fileName.substring(pos + 1);
				pos = key.lastIndexOf(".");
				if (pos > 0)
					return key.substring(0, pos);
			}
		}

		return null;
	}
	
	@Override
	protected Map<String, String> parseHeadersBySource(String fileName) {
		// PM201612220330+0800_20161222.0300+0800-0315+0800_CELLPOWER.xml
		//HEADER_KPI_DATE=20161222
		//HEADER_KPI_HOUR=0300
		Map<String, String> headers = new HashMap<String, String>();

		boolean flag = false;
		if (fileName != null) {
			int pos = fileName.indexOf("_");
			if (pos > 0) {
				String str = fileName.substring(pos + 1);
				pos = str.indexOf("+");
				if (pos > 0) {
					str = str.substring(0, pos);
					pos = str.indexOf(".");
					if (pos > 0) {
						String d = str.substring(0, pos);
						String h = str.substring(pos + 1, str.length());
						headers.put(HEADER_KPI_DATE, d);
						headers.put(HEADER_KPI_HOUR, h);
						flag = true;
					}
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
			return new ZTEKPIInterceptor(super.configFilePath, super.types);
		}
	}

	public static void main(String[] args) {
		String fileName = "PM201612220330+0800_20161222.0300+0800-0315+0800_CELLPOWER.xml";
		ZTEKPIInterceptor kpi = new ZTEKPIInterceptor(null,null);
		System.out.println(kpi.getTypeFromSource(fileName));
		System.out.println(kpi.parseHeadersBySource(fileName));
		
		String url = "http://st.browser.vivo.com.cn/blockDancing?u=1501004d41473247430598684c0a5100&imei=864510020848944&app_version=10010&elapsedtime=82849805&model=vivo+X3L&cs=0&app_package=com.android.browser&cfrom=155";
		System.out.println(url.substring("http://".length()));
	}
}

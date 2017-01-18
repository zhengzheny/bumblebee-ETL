package com.gsta.bigdata.etl.flume;

import org.apache.flume.Context;
import org.apache.flume.interceptor.Interceptor;

public class AbstractKPIBuilder implements Interceptor.Builder {
	protected String configFilePath;
	protected String[] types;

	@Override
	public void configure(Context context) {
		this.configFilePath = context.getString("configFilePath");
		String str = context.getString("types");
		if (str != null) {
			this.types = str.split(",", -1);
		}
	}

	@Override
	public Interceptor build() {
		return null;
	}
}

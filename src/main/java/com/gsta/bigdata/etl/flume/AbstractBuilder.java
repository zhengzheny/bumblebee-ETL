package com.gsta.bigdata.etl.flume;

import org.apache.flume.Context;
import org.apache.flume.interceptor.Interceptor;

public class AbstractBuilder implements Interceptor.Builder {
	protected String configFilePath;
	protected String configFileName;
	protected int fileCount;
	protected String headerFields;
	protected String fileNameHeaders;

	@Override
	public void configure(Context context) {
		this.configFilePath = context.getString("configFilePath");
		this.configFileName = context.getString("configFileName");
		this.fileNameHeaders = context.getString("fileNameHeaders");
		this.headerFields = context.getString("fieldsHeader");

		String str = context.getString("fileCount");
		try {
			this.fileCount = Integer.parseInt(str);
		} catch (NumberFormatException e) {
			this.fileCount = 500;
		}
	}

	@Override
	public Interceptor build() {
		return null;
	}
}

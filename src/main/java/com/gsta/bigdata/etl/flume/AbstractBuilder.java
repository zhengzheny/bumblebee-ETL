package com.gsta.bigdata.etl.flume;

import org.apache.flume.Context;
import org.apache.flume.interceptor.Interceptor;

public class AbstractBuilder implements Interceptor.Builder {
	protected String configFilePath;
	protected String configFileName;
	protected int fileCount;
	protected String headerFields;
	protected String fileNameHeaderType;
	
	public final static String CONFIG_FILE_PATH = "configFilePath";
	public final static String CONFIG_FILE_NAME = "configFileName";
	public final static String FILE_NAME_HEADERS = "fileNameHeaderType";
	public final static String FIELDS_HEADER = "fieldsHeader";
	public final static String FILE_COUNT = "fileCount";

	@Override
	public void configure(Context context) {
		this.configFilePath = context.getString(CONFIG_FILE_PATH);
		this.configFileName = context.getString(CONFIG_FILE_NAME);
		this.fileNameHeaderType = context.getString(FILE_NAME_HEADERS);
		this.headerFields = context.getString(FIELDS_HEADER);

		String str = context.getString(FILE_COUNT);
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

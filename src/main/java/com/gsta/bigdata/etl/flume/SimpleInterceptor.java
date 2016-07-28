package com.gsta.bigdata.etl.flume;

import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import com.gsta.bigdata.etl.ETLRunner;
import com.gsta.bigdata.etl.core.ETLProcess;

public class SimpleInterceptor extends AbstractInterceptor {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private String configFileName;
	private String configFilePath;
	private ETLProcess process = new ETLProcess();
	
	public SimpleInterceptor(String configFilePath,String configFileName, String fileNameHeaders,
			String headerFields, int fileCount) {
		super(fileNameHeaders, headerFields, fileCount);
		
		this.configFilePath = configFilePath;
		this.configFileName = configFileName;
	}

	@Override
	public void initialize() {
		super.initialize();
		
		String fileName = this.configFilePath;
		if(!this.configFilePath.endsWith("/")){
			fileName = fileName + "/";
		}
		fileName = fileName + this.configFileName;
		if(!fileName.endsWith(".xml")){
			fileName = fileName + ".xml";
		}
		
		logger.info("config file is " + fileName);
		Element processNode = new ETLRunner().getProcessNode(fileName,null);
		if (processNode == null) {
			throw new RuntimeException("get null process node...");
		}

		this.process.init(processNode);
	}

	@Override
	protected String getFileType(String fileName) {
		//simple flume interceptor don't need file type
		return null;
	}

	@Override
	protected ETLProcess getProcess(String fileType) {
		return this.process;
	}

	public static class Builder extends AbstractBuilder {
		@Override
		public Interceptor build() {
			return new SimpleInterceptor(super.configFilePath,super.configFileName,
					super.fileNameHeaders, super.headerFields, super.fileCount);
		}
	}
}

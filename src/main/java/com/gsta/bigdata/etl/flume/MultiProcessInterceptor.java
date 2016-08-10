package com.gsta.bigdata.etl.flume;

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import com.gsta.bigdata.etl.ETLRunner;
import com.gsta.bigdata.etl.core.ETLProcess;

/**
 * multi process interceptor
 * configFileName contains multi process xml,
 * Flume deal event by source file name which matchs process config file name
 * @author tianxq
 *
 */
public class MultiProcessInterceptor extends AbstractInterceptor {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private String configFileName;
	private String configFilePath;
	private Map<String, ETLProcess> processes = new HashMap<String, ETLProcess>();
	
	public MultiProcessInterceptor(String configFilePath,String configFileName,String fileNameHeaders, String headerFields,
			int fileCount) {
		super(fileNameHeaders, headerFields, fileCount);
		
		this.configFilePath = configFilePath;
		this.configFileName = configFileName;
	}

	@Override
	/**
	 * 
	 * @param fileName eg:ETL_LTEUP_HttpWap_201607251421.a.731.1171.csv.done
	 * @return ETL_LTEUP_HTTPWAP
	 */
	protected String getFileType(String fileName) {
		if(fileName == null){
			return null;
		}
		
		/*int pos = fileName.lastIndexOf("_");
		if (pos != -1) {
			return fileName.substring(0, pos).toUpperCase();
		}*/
		
		String deli = "_";
		String[] fields = fileName.split(deli,-1);
		if(fields.length >= 3){
			return (fields[0] + deli + fields[1] + deli + fields[2]).toUpperCase();
		}
		
		return null;
	}

	@Override
	protected ETLProcess getProcess(String fileType) {
		return this.processes.get(fileType);
	}

	@Override
	public void initialize() {
		super.initialize();
		
		if(!this.configFilePath.endsWith("/")){
			this.configFilePath = this.configFilePath + "/";
		}
		logger.info("config path=" + this.configFilePath);
		
		if(this.configFileName != null){
			String[] files = this.configFileName.split(",",-1);
			for(String file:files){
				String key = file;
				if(file.endsWith(".xml")){
					key = file.substring(0, file.indexOf(".xml"));
				}else{
					file = file + ".xml";
				}
				
				String fileName = this.configFilePath + file;
				logger.info("config file is " + fileName);
				Element processNode = new ETLRunner().getProcessNode(fileName,null);
				if (processNode == null) {
					throw new RuntimeException("get null process node...");
				}

				ETLProcess process = new ETLProcess();
				process.init(processNode);
				this.processes.put(key.toUpperCase(), process);
			}
 		}
	}

	public static class Builder extends AbstractBuilder {
		@Override
		public Interceptor build() {
			return new MultiProcessInterceptor(super.configFilePath,super.configFileName,
					super.fileNameHeaders, super.headerFields, super.fileCount);
		}
	}
}

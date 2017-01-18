package com.gsta.bigdata.etl.flume;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import com.gsta.bigdata.etl.ETLRunner;
import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.ETLProcess;

public abstract class AbstractKPIInterceptor implements Interceptor {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	protected String configFilePath;
	private String[] types;
	private Map<String, ETLProcess> processes = new HashMap<String, ETLProcess>();
	
	protected final static String HEADER_TYPE = "type";
	protected final static String HEADER_KPI_DATE = "kd";
	protected final static String HEADER_KPI_HOUR = "kh";
	protected final static String HEADER_DEFAULT_VALUE = "NULL";

	public AbstractKPIInterceptor(String configFilePath, String[] types) {
		super();
		
		this.configFilePath = configFilePath;
		this.types = types;
	}

	@Override
	public void initialize() {
		if (this.configFilePath == null || this.types == null
				|| this.types.length <= 0) {
			logger.error("invalid config file path or file names...");
			return;
		}

		if (!this.configFilePath.endsWith("/")) {
			this.configFilePath = this.configFilePath + "/";
		}
		logger.info("config path=" + this.configFilePath);

		for (String type : this.types) {
			String file = getConfigFileByType(type);

			logger.info("config file is " + type);
			Element processNode = new ETLRunner().getProcessNode(file, null);
			if (processNode == null) {
				throw new RuntimeException(type + " get null process node...");
			}

			ETLProcess process = new ETLProcess();
			process.init(processNode);
			this.processes.put(type, process);
		}
	}
	
	protected abstract String getConfigFileByType(String type);

	@Override
	public Event intercept(Event event) {
		if (event == null) {
			return null;
		}
		
		String fileName = event.getHeaders().get(
				SpoolDirectorySourceConstants.DEFAULT_BASENAME_HEADER_KEY);
		String type = this.getTypeFromSource(fileName);
		ETLProcess process = this.processes.get(type);
		if (process == null) {
			logger.error(fileName + " get null process");
			return null;
		}
		
		String line = new String(event.getBody());
		try {
			ETLData data = process.parseLine(line, null);
			if (data != null) {
				process.onTransform(data);
				String output = process.getOutputValue(data);

				if (output != null) {
					event.setBody(output.getBytes());

					Map<String, String> headers = event.getHeaders();
					headers.put(HEADER_TYPE, type);
					headers.putAll(this.parseHeadersBySource(fileName));
					return event;
				}
			}
		} catch (Exception e) {
			logger.error("dataline=" + line + ",error:" + e.getMessage());
		}

		return null;
	}
	
	protected abstract String getTypeFromSource(String fileName) ;
	
	protected abstract Map<String, String> parseHeadersBySource(String fileName);

	@Override
	public List<Event> intercept(List<Event> events) {
		List<Event> retEvents = new ArrayList<Event>();

		for (Event event : events) {
			Event interceptedEvent = intercept(event);  
            if (interceptedEvent != null) {  
            	retEvents.add(interceptedEvent);  
            }  
		}

		return retEvents;
	}

	@Override
	public void close() {
		
	}
}

package com.gsta.bigdata.etl;

import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import com.google.common.collect.Lists;
import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.ETLProcess;
import com.gsta.bigdata.etl.core.source.MroHuaWei;

/**
 * parse xml source file to csv and write output to HDFS. The output file
 * according to enodeid % 500
 * 
 * @author tianxq
 *
 */
public class FlumeMROInterceptor implements Interceptor {
	private Logger logger = LoggerFactory.getLogger(getClass());
	private String configFile;
	private String timeStampHeader;
	private String eNodeIdHeader;
	private int fileCount;
	private ETLProcess process = new ETLProcess();

	public FlumeMROInterceptor(String configFile, String timeStampHeader,
			String eNodeIdHeader,int fileCount) {
		super();
		this.configFile = configFile;
		this.timeStampHeader = timeStampHeader;
		this.eNodeIdHeader = eNodeIdHeader;
		this.fileCount = fileCount;
	}

	@Override
	public void initialize() {
		logger.info("config file is " + this.configFile);
		Element processNode = new ETLRunner().getProcessNode(this.configFile,null);
		if (processNode == null) {
			throw new RuntimeException("get null process node...");
		}

		process.init(processNode);
	}

	@Override
	public Event intercept(Event event) {
		if (event == null) {
			return null;
		}

		String line = new String(event.getBody());
		try {
			ETLData data = this.process.parseLine(line, null);
			if (data != null) {
				this.process.onTransform(data);
				String ret = this.process.getOutputValue(data);
				if (ret != null) {
					event.setBody(ret.getBytes());
					
					Map<String, String> headers = event.getHeaders();
					headers.put(this.timeStampHeader, this.getTimeStamp(data
							.getValue(MroHuaWei.FIELD_TIMESTAMP)));
					headers.put(this.eNodeIdHeader, this.getEnodeId(data
							.getValue(MroHuaWei.FIELD_ENODEBID)));

					return event;
				}
			}
		} catch (Exception e) {
			logger.error("dataline=" + line + ",error:" + e.getMessage());
		}

		return null;
	}
	
	//timeStamp = "2016-06-04 00:00:01.893";
	private String getTimeStamp(String timeStamp) {
		String ret = "unknown";
		if (timeStamp != null && timeStamp.contains(":")) {
			ret = timeStamp.substring(0, timeStamp.indexOf(":"))
					.replace('-', ' ').replace(" ", "");
		}

		return ret;
	}
	
	private String getEnodeId(String eNodeId){
		String ret = "unknown";
		if(eNodeId == null){
			return ret;
		}
		
		try{
			int id = Integer.parseInt(eNodeId);
			return String.valueOf(id % this.fileCount);
		}catch(NumberFormatException e){
			logger.warn("eNodeId is not number...");
		}
		
		return ret;
	}

	@Override
	public List<Event> intercept(List<Event> events) {
		List<Event> intercepted = Lists.newArrayListWithCapacity(events.size());
		
		for (Event event : events) {
			Event interceptedEvent = intercept(event);
			if (interceptedEvent != null) {
				intercepted.add(interceptedEvent);
			}
		}
		
		return intercepted;
	}

	@Override
	public void close() {

	}

	public static class Builder implements Interceptor.Builder {
		private String configFile;
		private String timeStampHeader;
		private String eNodeIdHeader;
		private int fileCount;

		@Override
		public void configure(Context context) {
			this.configFile = context.getString("configFile");
			this.timeStampHeader = context.getString("timeStampHeader");
			this.eNodeIdHeader = context.getString("eNodeIdHeader");
			
			String str = context.getString("fileCount");
			try{
				this.fileCount = Integer.parseInt(str);
			}catch(NumberFormatException e){
				this.fileCount = 500;
			}
		}

		@Override
		public Interceptor build() {
			return new FlumeMROInterceptor(this.configFile,
					this.timeStampHeader, this.eNodeIdHeader, this.fileCount);
		}
	}

	public static void  main(String[] args){
		String timeStamp = "2016-06-04 00:00:01.893";
		timeStamp = "aaaaa";
		String s = timeStamp.substring(0, timeStamp.indexOf(":")).replace('-', ' ').replace(" ", "");
		System.out.println(s);
	}
}

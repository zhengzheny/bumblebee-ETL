package com.gsta.bigdata.etl.flume;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import com.google.common.collect.Lists;
import com.gsta.bigdata.etl.ETLRunner;
import com.gsta.bigdata.etl.core.Constants;
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
	private static final Logger logger = LoggerFactory.getLogger(FlumeMROInterceptor.class);
	private String configFile;
	private String timeStampHeader;
	private String eNodeIdHeader;
	private String strHeaderFields;
	private String[] headerFields;
	private int fileCount;
	private ETLProcess process = new ETLProcess();
	//mode=0,single parse;mode=1,multi parse
	private int mode = 0;

	public FlumeMROInterceptor(String configFile, String timeStampHeader,
			String eNodeIdHeader,int fileCount,String headerFields) {
		super();
		this.configFile = configFile;
		this.timeStampHeader = timeStampHeader;
		this.eNodeIdHeader = eNodeIdHeader;
		this.fileCount = fileCount;
		this.strHeaderFields = headerFields;
	}

	@Override
	public void initialize() {
		logger.info("config file is " + this.configFile);
		Element processNode = new ETLRunner().getProcessNode(this.configFile,null);
		if (processNode == null) {
			throw new RuntimeException("get null process node...");
		}

		this.process.init(processNode);
		String mapperClass = this.process.getConf(Constants.HADOOP_MAPPER_CLASS);
		if(mapperClass != null && mapperClass.contains("MultiETLMapper")){
			this.mode = 1;
		}
		
		if(this.strHeaderFields != null){
			this.headerFields = this.strHeaderFields.split(",", -1);
			logger.info("head fields=" + this.strHeaderFields);
		}
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
				String output = this.process.getOutputValue(data);

				if (output != null) {
					this.buildEvent(event, output, data);
					return event;
				}
			}
		} catch (Exception e) {
			logger.error("dataline=" + line + ",error:" + e.getMessage());
		}

		return null;
	}
	
	private List<Event> multiIntercept(Event event) {
		if (event == null) {
			return null;
		}

		List<Event> retEvents = new ArrayList<Event>();
		String line = new String(event.getBody());
		//logger.info("fileName=" + event.getHeaders().get("basename"));
		try {
			List<ETLData> datas = this.process.parseLine(line);
			if (datas != null) {
				for (ETLData etlData : datas) {
					this.process.onTransform(etlData);
					String output = this.process.getOutputValue(etlData);

					Event tempEvent = new SimpleEvent();
					this.buildEvent(tempEvent, output, etlData);

					retEvents.add(tempEvent);
				}
			}
		} catch (Exception e) {
			logger.error("dataline=" + line + ",error:" + e.getMessage());
		}

		return retEvents;
	}
	
	private void buildEvent(Event event, String output, ETLData etlData) {
		if (event == null || output == null || etlData == null) {
			return;
		}

		event.setBody(output.getBytes());

		Map<String, String> headers = event.getHeaders();
		if (this.timeStampHeader != null) {
			headers.put(this.timeStampHeader, this.getTimeStamp(etlData
					.getValue(MroHuaWei.FIELD_TIMESTAMP)));
		}
		
		if (this.eNodeIdHeader != null) {
			headers.put(this.eNodeIdHeader,
					this.getEnodeId(etlData.getValue(MroHuaWei.FIELD_ENODEBID)));
		}
		
		if(this.headerFields != null){
			for(String field:this.headerFields){
				headers.put(field, etlData.getValue(field));
			}
		}
	}
	
	@Override
	public List<Event> intercept(List<Event> events) {
		List<Event> intercepted = Lists.newArrayListWithCapacity(events.size());

		for (Event event : events) {
			switch (this.mode) {
			case 0:
				Event interceptedEvent = this.intercept(event);
				if (interceptedEvent != null) {
					intercepted.add(interceptedEvent);
				}
				break;
			case 1:
				List<Event> lstEvent = this.multiIntercept(event);
				if (lstEvent != null && lstEvent.size() > 0) {
					intercepted.addAll(lstEvent);
				}
				break;
			}
		}

		return intercepted;
	}

	@Override
	public void close() {

	}
	
	// timeStamp = "2016-06-04 00:00:01.893";
	private String getTimeStamp(String timeStamp) {
		String ret = "unknown";
		if (timeStamp != null && timeStamp.contains(":")) {
			ret = timeStamp.substring(0, timeStamp.indexOf(":"))
					.replace('-', ' ').replace(" ", "");
		}

		return ret;
	}

	private String getEnodeId(String eNodeId) {
		String ret = "unknown";
		if (eNodeId == null) {
			return ret;
		}

		try {
			int id = Integer.parseInt(eNodeId);
			return String.valueOf(id % this.fileCount);
		} catch (NumberFormatException e) {
			logger.warn("eNodeId is not number...");
		}

		return ret;
	}

	public static class Builder implements Interceptor.Builder {
		private String configFile;
		private String timeStampHeader;
		private String eNodeIdHeader;
		private int fileCount;
		private String headerFields;

		@Override
		public void configure(Context context) {
			this.configFile = context.getString("configFile");
			this.timeStampHeader = context.getString("timeStampHeader");
			this.eNodeIdHeader = context.getString("eNodeIdHeader");
			this.headerFields = context.getString("headerFields");
			
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
					this.timeStampHeader, this.eNodeIdHeader, this.fileCount,
					this.headerFields);
		}
	}
}
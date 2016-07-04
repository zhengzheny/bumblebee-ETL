package com.gsta.bigdata.etl;

import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import com.google.common.collect.Lists;
import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.ETLProcess;

/**
 * parse xml source file to csv and write output to HDFS. The output file
 * according to enodeid % 500
 * 
 * @author tianxq
 *
 */
public class FlumeInterceptor implements Interceptor {
	private Logger logger = LoggerFactory.getLogger(getClass());
	private String configFile;
	private ETLProcess process = new ETLProcess();

	public FlumeInterceptor(String configFile) {
		super();
		this.configFile = configFile;
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
					return event;
				}
			}
		} catch (Exception e) {
			logger.error("dataline=" + line + ",error:" + e.getMessage());
		}

		return null;
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

		@Override
		public void configure(Context context) {
			this.configFile = context.getString("configFile");
		}

		@Override
		public Interceptor build() {
			return new FlumeInterceptor(this.configFile);
		}
	}

}

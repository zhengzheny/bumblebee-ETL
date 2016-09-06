package com.gsta.bigdata.etl.flume;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * parse xml source file to csv and write output to HDFS. The output file
 * according to enodeid % 500
 * 
 * @author tianxq
 *
 */
public class FileNameHeaderInterceptor implements Interceptor {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	
	private IFileNameHeader fileNameHeader;
	private String fileNameHeaderType;

	public FileNameHeaderInterceptor(String fileNameHeaders) {
		super();
		
		this.fileNameHeaderType = fileNameHeaders;

		logger.info("fileName header:" + this.fileNameHeaderType);
	}

	@Override
	public void initialize() {
		if (this.fileNameHeaderType != null) {
			try {
				this.fileNameHeader = new FileNameHeaderFactory().getClass(
						this.fileNameHeaderType).newInstance();
			} catch (Exception e) {
				throw new RuntimeException(e.toString());
			}
		}
	}

	@Override
	public Event intercept(Event event) {
		if (event == null) {
			return null;
		}

		String fileName = event.getHeaders().get(
				SpoolDirectorySourceConstants.DEFAULT_BASENAME_HEADER_KEY);

		Map<String, String> headers = event.getHeaders();
		if (this.fileNameHeader != null && fileName != null) {
			headers.putAll(this.fileNameHeader.parseHeaders(fileName));
		}

		return event;
	}

	@Override
	public List<Event> intercept(List<Event> events) {
		List<Event> retEvents = new ArrayList<Event>();

		for (Event event : events) {
			Event tempEvent = this.intercept(event);
			if(tempEvent != null){
			retEvents.add(tempEvent);
			}
		}

		return retEvents;
	}

	@Override
	public void close() {

	}
	
	public static class Builder implements Interceptor.Builder {
		private String fileNameHeaderType;
		
		@Override
		public void configure(Context context) {
			this.fileNameHeaderType = context.getString(AbstractBuilder.FILE_NAME_HEADERS);
			
		}
		@Override
		public Interceptor build() {
			return new FileNameHeaderInterceptor(this.fileNameHeaderType);
		}
	}
}
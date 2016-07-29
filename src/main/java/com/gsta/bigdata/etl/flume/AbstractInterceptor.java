package com.gsta.bigdata.etl.flume;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.ETLProcess;

/**
 * parse xml source file to csv and write output to HDFS. The output file
 * according to enodeid % 500
 * 
 * @author tianxq
 *
 */
public abstract class AbstractInterceptor implements Interceptor {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final static String HEADER_BASENAME = "basename";
	
	private FieldsHeader fieldsHeader;
	private IFileNameHeader fileNameHeader;
	private String fileNameHeaderType;

	public AbstractInterceptor(String fileNameHeaders, String headerFields,
			int fileCount) {
		super();
		
		this.fieldsHeader = new FieldsHeader(headerFields, fileCount);
		this.fileNameHeaderType = fileNameHeaders;

		logger.info("fileName header:" + this.fileNameHeaderType);
		logger.info("fields header:" + headerFields);
		logger.info("fileCount:" + fileCount);
	}

	protected abstract String getFileType(String fileName);

	protected abstract ETLProcess getProcess(String fileType);

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

		String fileName = event.getHeaders().get(HEADER_BASENAME);
		String fileType = getFileType(fileName);
		ETLProcess process = getProcess(fileType);
		if (process == null) {
			return null;
		}

		String line = new String(event.getBody());
		try {
			ETLData data = process.parseLine(line, null);
			if (data != null) {
				process.onTransform(data);
				String output = process.getOutputValue(data);

				if (output != null) {
					this.buildEvent(event, output, data, fileName);
					return event;
				}
			}
		} catch (Exception e) {
			logger.error("dataline=" + line + ",error:" + e.getMessage());
		}

		return null;
	}

	private Event singleETL(Event event, ETLProcess process,
			String fileName) {
		if (event == null || process == null) {
			return null;
		}

		String line = new String(event.getBody());
		try {
			ETLData data = process.parseLine(line, null);
			if (data != null) {
				process.onTransform(data);
				String output = process.getOutputValue(data);

				if (output != null) {
					this.buildEvent(event, output, data, fileName);
					return event;
				}
			}
		} catch (Exception e) {
			logger.error("dataline=" + line + ",error:" + e.getMessage());
		}

		return null;
	}

	private List<Event> multiETL(Event event, ETLProcess process,
			String fileName) {
		if (event == null || process == null) {
			return null;
		}

		List<Event> retEvents = new ArrayList<Event>();
		String line = new String(event.getBody());
		try {
			List<ETLData> datas = process.parseLine(line);
			if (datas != null) {
				for (ETLData etlData : datas) {
					process.onTransform(etlData);
					String output = process.getOutputValue(etlData);

					Event tempEvent = new SimpleEvent();
					this.buildEvent(tempEvent, output, etlData, fileName);

					retEvents.add(tempEvent);
				}
			}
		} catch (Exception e) {
			logger.error("dataline=" + line + ",error:" + e.getMessage());
		}

		return retEvents;
	}

	@Override
	public List<Event> intercept(List<Event> events) {
		List<Event> retEvents = new ArrayList<Event>();

		for (Event event : events) {
			String fileName = event.getHeaders().get(HEADER_BASENAME);
			String fileType = getFileType(fileName);
			ETLProcess process = getProcess(fileType);
			if (process == null) {
				continue;
			}

			switch (process.getMode()) {
			case 0:
				Event etlEvent = this.singleETL(event, process,fileName);
				if (etlEvent != null) {
					retEvents.add(etlEvent);
				}
				break;
			case 1:
				List<Event> lstEvent = this.multiETL(event, process,fileName);
				if (lstEvent != null && lstEvent.size() > 0) {
					retEvents.addAll(lstEvent);
				}
				break;
			}
		}

		return retEvents;
	}

	private void buildEvent(Event event, String output, ETLData etlData,
			String fileName) {
		if (event == null || output == null) {
			return;
		}

		event.setBody(output.getBytes());

		Map<String, String> headers = event.getHeaders();
		if (this.fileNameHeader != null && fileName != null) {
			headers.putAll(this.fileNameHeader.parseHeaders(fileName));
		}

		if (this.fieldsHeader != null && etlData != null) {
			headers.putAll(this.fieldsHeader.parseHeaders(etlData));
		}
	}

	@Override
	public void close() {

	}
}
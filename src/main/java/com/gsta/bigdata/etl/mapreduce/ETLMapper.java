package com.gsta.bigdata.etl.mapreduce;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.gsta.bigdata.etl.AbstractException;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.lookup.LKPTableMgr;
import com.gsta.bigdata.etl.core.process.MRProcess;
import com.gsta.bigdata.etl.core.source.ValidatorException;
import com.gsta.bigdata.utils.BeansUtils;
import com.gsta.bigdata.utils.HdfsUtils;

/**
 * the map of mapreduce
 * 
 * @author tianxq
 * 
 */
public class ETLMapper extends Mapper<Object, Text, Text, Text> {
	private MRProcess process;

	private Text txtKey = new Text();
	private Text txtValue = new Text();
	private final static String SCOPE = "map";

	private Logger logger = LoggerFactory.getLogger(getClass());

	// how many error records for writing error file
	private int errorRecordThreshold = 1000;

	private OutputStream errorOutput;
	// save error line set,if have multiple errors in the same line,write once
	private Set<String> errorRecords = new CopyOnWriteArraySet<String>();
	// error file name
	private String errorFileName;

	private OutputStream invalidOutput;
	// invalid records after verify,if have multiple invalid error in the same
	// line,write once
	private Set<String> invalidRecords = new CopyOnWriteArraySet<String>();
	// invalid file name
	private String invalidFileName;

	private OutputStream errorInfoOutput;
	// error information file name
	private String errorInfoFileName;
	// key is error code
	private Map<String, ErrorCodeCount> errorInfos = new ConcurrentHashMap<String, ErrorCodeCount>();

	@Override
	protected void setup(Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);

		String json = context.getConfiguration().get(
				Constants.HADOOP_CONF_MRPROCESS);
		this.process = (MRProcess) BeansUtils.json2obj(json, MRProcess.class);
		if (this.process == null) {
			throw new InterruptedException("process is null.");
		}

		// get lookup
		json = context.getConfiguration().get(Constants.PATH_LOOKUP);
		LKPTableMgr lkpTableMgr = BeansUtils.json2obj(json, LKPTableMgr.class);
		if (lkpTableMgr != null) {
			LKPTableMgr.getInstance().clone(lkpTableMgr);
		}

		String errorPath = this.process.getErrorPath();
		String taskId = context.getConfiguration().get(
				"mapreduce.task.attempt.id");
		// don't use File.separator,or don't pass test in windows platform
		this.errorFileName = errorPath + "/error." + taskId;
		this.invalidFileName = errorPath + "/invalid." + taskId;

		String outputPath = this.process.getOutputPath();
		this.errorInfoFileName = outputPath + "/errorInfo." + taskId;

		HdfsUtils hdfsUtils = new HdfsUtils();
		this.errorOutput = hdfsUtils.open(this.errorFileName);
		this.invalidOutput = hdfsUtils.open(this.invalidFileName);
		this.errorInfoOutput = hdfsUtils.open(this.errorInfoFileName);

		String thresholdCount = process
				.getConf(Constants.CF_ERROR_RECORD_WRITE_COUNT);
		if (thresholdCount != null && !"".equals(thresholdCount)) {
			this.errorRecordThreshold = Integer.parseInt(thresholdCount);
		}
	}

	@Override
	protected void map(Object key, Text value,
			Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		if(value == null || "".equals(value)){
			return;
		}
		
		ETLData data = null;

		try {
			data = this.process.parseLine(value.toString(), this.invalidRecords);
		} catch (ETLException e) {
			logger.error("dataline=" + value.toString() + ",error:"
					+ e.getMessage());

			//record error information
			this.recordErrorInfo(e);
			
			// write error file
			this.errorRecords.add(value.toString());
			if (this.errorRecords.size() >= this.errorRecordThreshold) {
				writeFiles(this.errorOutput, this.errorRecords,
						this.errorFileName);

			}

			// if occuring parsing exception,write error file and don't make transform
			return;
		} catch (ValidatorException e) {
			logger.error("dataline=" + value.toString() + ",error:"
					+ e.getMessage());
			
			//record error information
			this.recordErrorInfo(e);

			// write invalid file
			this.invalidRecords.add(value.toString());
			if (this.invalidRecords.size() >= this.errorRecordThreshold) {
				writeFiles(this.invalidOutput, this.invalidRecords,
						this.invalidFileName);
			}
			return;
		}

		try {
			if (null != data) {
				this.process.onTransform(data, ETLMapper.SCOPE);

				String outputKey = this.process.getOutputKey(data);
				this.txtKey.set(outputKey);

				String outputValue = this.process.getOutputValue(data);
				this.txtValue.set(outputValue);

				context.write(this.txtKey, this.txtValue);
			}
		} catch (ETLException e) {
			logger.error("dataline=" + value.toString() + ",error:"
					+ e.getMessage());
			
			//record error information
			this.recordErrorInfo(e);

			// write error file
			this.errorRecords.add(value.toString());
			if (this.errorRecords.size() >= this.errorRecordThreshold) {
				writeFiles(this.errorOutput, this.errorRecords,
						this.errorFileName);
			}
		}
	}

	private void recordErrorInfo(AbstractException e) {
		String errorCode = e.getErrorCode();
		String errorMessage = e.getMessage();
		//if map key contain error code,error count add one,else create new ErrorCodeCount
		if (this.errorInfos.containsKey(errorCode)) {
			this.errorInfos.get(errorCode).addCount();
		} else {
			ErrorCodeCount errorCodeCount = new ErrorCodeCount(errorCode,errorMessage);
			this.errorInfos.put(errorCode, errorCodeCount);
		}
		
	}

	private void writeFiles(OutputStream output, Set<String> records,
			String fileName) {
		if (records.size() <= 0 || output == null) {
			return;
		}

		int count = 0;
		Iterator<String> iter = records.iterator();
		while (iter.hasNext()) {
			try {
				String line = iter.next();
				output.write(line.getBytes("utf-8"));
				output.write("\r\n".getBytes());

				records.remove(line);
				count++;
			} catch (IOException e) {
				logger.error("write error file:" + e.getMessage());
			}
		}

		try {
			output.flush();
		} catch (IOException e) {
			logger.error("flush error file:" + e.getMessage());
		}

		logger.info("write error file:" + fileName + ",record count=" + count);
	}

	@Override
	protected void cleanup(Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);

		// write remaining error record
		writeFiles(this.errorOutput, this.errorRecords, this.errorFileName);
		writeFiles(this.invalidOutput, this.invalidRecords,
				this.invalidFileName);

		// write error information to HDFS file,and the main thread will
		//read them and print error info to console
		writeFiles(this.errorInfoOutput, this.getErrorInfo(),
				this.errorInfoFileName);

		if (this.errorOutput != null) {
			IOUtils.closeStream(this.errorOutput);
		}

		if (this.invalidOutput != null) {
			IOUtils.closeStream(this.invalidOutput);
		}
		
		if(this.errorInfoOutput != null){
			IOUtils.closeStream(this.errorInfoOutput);
		}
	}

	private Set<String> getErrorInfo() {
		Set<String> errorInfo = new HashSet<String>();
		
		for(ErrorCodeCount errorCodeCount:this.errorInfos.values()) {
			String info = null;
			try {
				info = BeansUtils.obj2json(errorCodeCount);
			} catch (JsonProcessingException e) {
				e.printStackTrace();
				logger.error(e.toString());
			}
			errorInfo.add(info);
		}
		
		return errorInfo;
	}
}

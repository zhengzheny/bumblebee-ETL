package com.gsta.bigdata.etl.mapreduce;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.gsta.bigdata.etl.AbstractException;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.ETLProcess;
import com.gsta.bigdata.etl.core.IRuleMgr;
import com.gsta.bigdata.etl.core.GeneralRuleMgr;
import com.gsta.bigdata.etl.core.lookup.LookupMgr;
import com.gsta.bigdata.etl.core.source.ValidatorException;
import com.gsta.bigdata.utils.BeansUtils;

/**
 * the map of mapreduce
 * 
 * @author tianxq
 * 
 */
public class ETLMapper extends Mapper<Object, Text, Text, Text> {
	protected ETLProcess process;

	protected Text txtKey = new Text();
	protected Text txtValue = new Text();
	private Text outText = new Text();  

	private Logger logger = LoggerFactory.getLogger(getClass());

	// how many error records for writing error file
	protected int errorRecordThreshold = 1000;

	// save error line set,if have multiple errors in the same line,write once
	protected Set<String> errorRecords = new CopyOnWriteArraySet<String>();
	// invalid records after verify,if have multiple invalid error in the same
	// line,write once
	protected Set<String> invalidRecords = new CopyOnWriteArraySet<String>();
	// key is error code
	private Map<String, ErrorCodeCount> errorInfos = new ConcurrentHashMap<String, ErrorCodeCount>();
	
	private MultipleOutputs<Text, Text> multiOutput ; 
	protected String errorPath;
	protected String outputPath;
	protected String encoding;
	
	@Override
	protected void setup(Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		
		multiOutput = new MultipleOutputs<Text, Text>(context);
		
		String json = context.getConfiguration().get(
				Constants.HADOOP_CONF_ETLPROCESS);
		this.process = (ETLProcess) BeansUtils.json2obj(json, ETLProcess.class);
		if (this.process == null) {
			throw new InterruptedException("process is null.");
		}

		json = context.getConfiguration().get(Constants.JSON_RULE_STATIS_MGR);
		GeneralRuleMgr ruleStatisMgr = BeansUtils.json2obj(json, GeneralRuleMgr.class);
		if(ruleStatisMgr != null){
			GeneralRuleMgr.getInstance().clone(ruleStatisMgr);
		}
		
		json = context.getConfiguration().get(Constants.JSON_LOOKUP_MGR);
		LookupMgr lookupMgr = BeansUtils.json2obj(json, LookupMgr.class);
		if(lookupMgr != null){
			LookupMgr.getInstance().clone(lookupMgr);
		}

		this.errorPath = this.process.getErrorPath();
		this.outputPath = this.process.getOutputPath();
		String thresholdCount = process
				.getConf(Constants.CF_ERROR_RECORD_WRITE_COUNT);
		if (thresholdCount != null && !"".equals(thresholdCount)) {
			this.errorRecordThreshold = Integer.parseInt(thresholdCount);
		}
		
		this.encoding = process.getConf(Constants.CF_SOURCE_ENCODING);
	}
	
	/**
	 * if file contains chinese,need transform the file encoding to UTF-8
	 * @param text
	 * @param encoding must be the same and file encoding, otherwise it will be converted into garbled
	 * @return
	 */
	protected Text transformTextToUTF8(Text text, String encoding)
			throws UnsupportedEncodingException {
		String value = new String(text.getBytes(), 0, text.getLength(),
				encoding);

		return new Text(value);
	}

	@Override
	protected void map(Object key, Text value_,
			Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		if(value_ == null || "".equals(value_.toString())){
			return;
		}
		
		Text value = value_;
		if(this.encoding != null){
			try{
				value = this.transformTextToUTF8(value_,this.encoding);
			}catch(UnsupportedEncodingException e){
				logger.error(e.toString());
				return;
			}
		}
		
		ETLData data = null;
		try {
			data = this.process.parseLine(value.toString(), this.invalidRecords);
		} catch (ETLException e) {
			logger.error("dataline=" + value.toString() + ",error:" + e.getMessage());

			//record error information
			this.recordErrorInfo(e);
			
			// write error file
			this.errorRecords.add(value.toString());
			if (this.errorRecords.size() >= this.errorRecordThreshold) {
				//writeFiles(this.errorOutput, this.errorRecords,this.errorFileName);
				this.writeFiles(this.errorPath, Constants.OUTPUT_ERROR_FILE_PREFIX, errorRecords);
			}

			// if occuring parsing exception,write error file and don't make transform
			return;
		} catch (ValidatorException e) {
			logger.error("dataline=" + value.toString() + ",error:" + e.getMessage());
			
			//record error information
			this.recordErrorInfo(e);

			// write invalid file
			this.invalidRecords.add(value.toString());
			if (this.invalidRecords.size() >= this.errorRecordThreshold) {
				//writeFiles(this.invalidOutput, this.invalidRecords,this.invalidFileName);
				this.writeFiles(this.errorPath, Constants.OUTPUT_INVALID_FILE_PREFIX, invalidRecords);
			}
			return;
		}

		try {
			if (null != data) {
				this.process.onTransform(data);

				String outputKey = this.process.getOutputKey(data);
				this.txtKey.set(outputKey);

				String outputValue = this.process.getOutputValue(data);
				this.txtValue.set(outputValue);

				context.write(this.txtKey, this.txtValue);
			}
		} catch (ETLException e) {
			logger.error("dataline=" + value.toString() + ",error:" + e.getMessage());
			
			//record error information
			this.recordErrorInfo(e);

			// write error file
			this.errorRecords.add(value.toString());
			if (this.errorRecords.size() >= this.errorRecordThreshold) {
				//writeFiles(this.errorOutput, this.errorRecords,this.errorFileName);
				this.writeFiles(this.errorPath, Constants.OUTPUT_ERROR_FILE_PREFIX, errorRecords);
			}
		}
	}

	protected void recordErrorInfo(AbstractException e) {
		String errorCode = e.getErrorCode();
		String errorMessage = e.getMessage();
		
		if(errorCode == null || errorCode.trim().length() <=0){
			logger.error("errorCode:" + errorCode + ",errorMessage:" + errorMessage);
			return;
		}
		
		//if map key contain error code,error count add one,else create new ErrorCodeCount
		if (this.errorInfos.containsKey(errorCode)) {
			this.errorInfos.get(errorCode).addCount();
		} else {
			ErrorCodeCount errorCodeCount = new ErrorCodeCount(errorCode,errorMessage);
			this.errorInfos.put(errorCode, errorCodeCount);
		}
		
	}
	
	private Set<String> getErrorInfo() {
		Set<String> errorInfo = new CopyOnWriteArraySet<String>();
		
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
	
	protected void writeFiles(String dir, String namedOutput, Set<String> records)
			throws IOException, InterruptedException {
		if (namedOutput == null || dir == null) {
			logger.error("write output file,namedOutput or dir is null.");
			return;
		}

		if (records == null || records.size() <= 0) {
			return;
		}

		int count = 0;
		StringBuffer sb = new StringBuffer();
		Iterator<String> iter = records.iterator();
		while (iter.hasNext()) {
			String line = iter.next();
			sb.append(line).append("\r\n");

			records.remove(line);
			count++;
		}

		outText.set(sb.toString());
		if(dir.endsWith("/") || dir.endsWith("\\")){
			dir = dir + namedOutput;
		}else{
			dir = dir + "/" + namedOutput;
		}
		
		this.multiOutput.write(namedOutput, outText, null, dir);
		
		logger.info("write file,dir=" + dir + ",file prefix=" + namedOutput
				+ ",record count=" + count);
	}

	@Override
	protected void cleanup(Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);

		// write remaining error record
		this.writeFiles(this.errorPath, Constants.OUTPUT_ERROR_FILE_PREFIX, errorRecords);
		this.writeFiles(this.errorPath, Constants.OUTPUT_INVALID_FILE_PREFIX, invalidRecords);
		
		// write error information to HDFS file,and the main thread will
		//read them and print error info to console
		this.writeFiles(this.outputPath, Constants.OUTPUT_ERROR_INFO_FILE_PREFIX,  this.getErrorInfo());

		this.writeRuleStatis();
		
		multiOutput.close();
	}

	private void writeRuleStatis() throws IOException, InterruptedException {
		Map<String,IRuleMgr> ruleMgrs = GeneralRuleMgr.getInstance().getRuleMgrs();
		for (IRuleMgr ruleMgr : ruleMgrs.values()) {
			if (ruleMgr == null || ruleMgr.getDpiRule() == null) {
				continue;
			}
			
			StringBuffer sb = new StringBuffer();
			sb.append("=========rule statistical information============\r\n");
			sb.append(ruleMgr.getStatInfo()).append("\r\n");

			sb.append("==========rule matched information===============\r\n");
			Map<String, Long> ruleStatis = ruleMgr.getRuleMatchedStats();
			if(ruleStatis != null){
				for (Map.Entry<String, Long> mapEntry : ruleStatis.entrySet()) {
					String key = (String) mapEntry.getKey();
					Long value = (Long) mapEntry.getValue();
					sb.append(value.longValue()).append("\t").append(key)
							.append("\r\n");
				}
			}

			String dir = ruleMgr.getStatisFileDir();
			if (dir == null) {
				dir = this.process.getOutputPath() + "/ruleStatis";
			}
			if (dir.endsWith("/") || dir.endsWith("\\")) {
				dir = dir + ruleMgr.getId();
			} else {
				dir = dir + "/" + ruleMgr.getId();
			}
			this.multiOutput.write(ruleMgr.getId(), sb.toString(), null,dir);
		}// end for ruleMgrs
	}
}

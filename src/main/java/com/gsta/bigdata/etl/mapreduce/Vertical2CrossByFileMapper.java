package com.gsta.bigdata.etl.mapreduce;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.source.ValidatorException;

public class Vertical2CrossByFileMapper extends ETLMapper {
	private Logger logger = LoggerFactory.getLogger(getClass());
	protected ConcurrentHashMap<String,HashMap<String,String>> crossDatas = new ConcurrentHashMap<String,HashMap<String,String>>();
	
	@Override
	protected void map(Object key, Text value_,
			Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		if(value_ == null || "".equals(value_.toString())){
			return;
		}
		
		Text value = value_;
		if(super.encoding != null){
			try{
				value = super.transformTextToUTF8(value_,super.encoding);
			}catch(UnsupportedEncodingException e){
				logger.error(e.toString());
				return;
			}
		}
		
		ETLData data = null;
		try {
			data = super.process.parseLine(value.toString(), super.invalidRecords);
			//add filename to output if you need
			if (super.outputSourceFileName) {
				data.addData(Constants.OUTPUT_FIELD_FILE_NAME, super.sourceFileName);
			}
		} catch (ETLException e) {
			logger.error("dataline=" + value.toString() + ",error:" + e.getMessage());

			//record error information
			super.recordErrorInfo(e);
			
			// write error file
			super.errorRecords.add(value.toString());
			if (super.errorRecords.size() >= super.errorRecordThreshold) {
				//writeFiles(this.errorOutput, this.errorRecords,this.errorFileName);
				super.writeFiles(super.errorPath, Constants.OUTPUT_ERROR_FILE_PREFIX, errorRecords);
			}

			// if occuring parsing exception,write error file and don't make transform
			return;
		} catch (ValidatorException e) {
			logger.error("dataline=" + value.toString() + ",error:" + e.getMessage());
			
			//record error information
			super.recordErrorInfo(e);

			// write invalid file
			super.invalidRecords.add(value.toString());
			if (super.invalidRecords.size() >= super.errorRecordThreshold) {
				//writeFiles(this.invalidOutput, this.invalidRecords,this.invalidFileName);
				super.writeFiles(super.errorPath, Constants.OUTPUT_INVALID_FILE_PREFIX, invalidRecords);
			}
			return;
		}
		
		if(data != null){
			String dataKey = data.getValue(Constants.FIELD_KEY);
			if(this.crossDatas.containsKey(dataKey)){
				this.crossDatas.get(dataKey).putAll(data.getData());
			}else{
				HashMap<String,String> temp = new HashMap<String,String>();
				temp.putAll(data.getData());
				this.crossDatas.put(dataKey, temp);
			}
		}
	}

	@Override
	protected void cleanup(Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);

		for (HashMap<String, String> value : this.crossDatas.values()) {
			ETLData etlData = new ETLData();
			etlData.addData(value);

			super.process.onTransform(etlData);

			String outputKey = super.process.getOutputKey(etlData);
			super.txtKey.set(outputKey);

			String outputValue = super.process.getOutputValue(etlData);
			super.txtValue.set(outputValue);

			context.write(super.txtKey, super.txtValue);
		}
	}
}

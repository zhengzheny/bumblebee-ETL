package com.gsta.bigdata.etl.mapreduce;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.source.ValidatorException;

public class MultiETLMapper extends ETLMapper {
	private Logger logger = LoggerFactory.getLogger(getClass());
	
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
		
		List<ETLData> datas = null;
		try {
			datas = super.process.parseLine(value.toString());
		} catch (ETLException e) {
			logger.error("dataline=" + value.toString() + ",error:" + e.getMessage());

			//record error information
			super.recordErrorInfo(e);
			
			// write error file
			super.errorRecords.add(value.toString());
			if (super.errorRecords.size() >= super.errorRecordThreshold) {
				//writeFiles(this.errorOutput, this.errorRecords,this.errorFileName);
				super.writeFiles(super.errorPath, Constants.OUTPUT_ERROR_FILE_PREFIX, super.errorRecords);
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
				super.writeFiles(super.errorPath, Constants.OUTPUT_INVALID_FILE_PREFIX, super.invalidRecords);
			}
			return;
		}

		try {
			if (null != datas) {
				for (ETLData etlData : datas) {
					super.process.onTransform(etlData);

					String outputKey = this.process.getOutputKey(etlData);
					this.txtKey.set(outputKey);

					String outputValue = this.process.getOutputValue(etlData);
					this.txtValue.set(outputValue);

					context.write(this.txtKey, this.txtValue);
				}
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
}

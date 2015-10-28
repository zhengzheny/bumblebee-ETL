package com.gsta.bigdata.etl.localFile;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.LocalFileRunner.InputFile;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ETLProcess;
import com.gsta.bigdata.etl.core.OutputMetaData;

/**
 * abstract handler 
 * 
 * @author tianxq
 *
 */
public abstract class AbstractHandler {
	protected ETLProcess process;

	// the result queue
	protected Set<String> queue = new CopyOnWriteArraySet<String>();
	protected Set<String> errorRecords = new CopyOnWriteArraySet<String>();
	protected Set<String> invalidRecords = new CopyOnWriteArraySet<String>();

	protected String errorFileName;
	protected String invalidFileName;
	protected String queueFileName;

	protected int errorRecordThreshold = 1000;
	protected int recordThreshold = 1000;

	// handler deal file
	protected InputFile inputFile;

	protected BufferedWriter errorOutStream;
	protected BufferedWriter invalidOutStream;
	protected BufferedWriter queueOutStream;

	protected String outputCharset = "utf-8";

	private Logger logger = LoggerFactory.getLogger(getClass());

	// handler deal record count
	protected long recordCount;
	// handler deal error count
	protected long errorCount;
	protected String fileName;
	
	private String outputFileName;
	//handler the file size or slice size;
	private BigDecimal taskSize;
	private AtomicLong count = new AtomicLong();
	private int lastPercent = 0;
	
	public AbstractHandler(ETLProcess process) {
		this.process = process;
	}
	
	/**
	 * if one file has one thread,index=-1,
	 * or set thread index
	 * @param index
	 */
	public void setOuputFileName(int index) {
		this.fileName = inputFile.getFile().getName();
		this.fileName = fileName.substring(0, fileName.lastIndexOf("."));
		//if process is slice local file,every slice has different file name.
		if(index >= 0){
			this.fileName = this.fileName + "-" + index;
		}
	}
	
	public void setup() throws ETLException {
		if (this.process == null) {
			throw new ETLException("process is null.");
		}

		String errorPath = this.process.getErrorPath();
		String outputPath = this.process.getOutputPath();
		File tempFile = new File(errorPath);
		if (!tempFile.exists()) {
			tempFile.mkdirs();
		}
		tempFile = new File(outputPath);
		if (!tempFile.exists()) {
			tempFile.mkdirs();
		}

		if (this.inputFile == null) {
			throw new ETLException("handler file is null.");
		}

		String fileSuffix = "txt";
		OutputMetaData outputMetaData = this.process.getOutputMetaData();
		if (outputMetaData != null) {
			fileSuffix = outputMetaData.getFileSuffix();
			this.outputCharset = outputMetaData.getCharset();
		}
		this.outputFileName = this.fileName + "." + fileSuffix;
		
		this.queueFileName = outputPath + "/" + fileName + "." + fileSuffix;
		this.errorFileName = errorPath + "/" + fileName + ".error";
		this.invalidFileName = errorPath + "/" + fileName + ".invalid";

		try {
			this.queueOutStream = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(this.queueFileName),this.outputCharset));

			this.errorOutStream = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(this.errorFileName),this.outputCharset));

			this.invalidOutStream = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(this.invalidFileName),this.outputCharset));
		} catch (IOException e) {
			throw new ETLException(e);
		}

		String thresholdCount = process.getConf(Constants.CF_LOCAL_FILE_WRITE_COUNT);
		if (thresholdCount != null && !"".equals(thresholdCount)) {
			this.recordThreshold = Integer.parseInt(thresholdCount);
		}

		thresholdCount = process.getConf(Constants.CF_ERROR_RECORD_WRITE_COUNT);
		if (thresholdCount != null && !"".equals(thresholdCount)) {
			this.errorRecordThreshold = Integer.parseInt(thresholdCount);
		}

		this.recordCount = 0;
	}

	
	public void handle(String line) throws ETLException{
		if(line == null || "".equals(line)){
			return;
		}
		
		this.count.addAndGet(line.length());
		
		BigDecimal bdCount = new BigDecimal(this.count.longValue()).divide(
				this.taskSize, 2, BigDecimal.ROUND_HALF_UP);
		bdCount = bdCount.multiply(new BigDecimal(100));
		int percent = bdCount.intValue();
		int deltaPercent = percent - this.lastPercent;
		if(deltaPercent >= 5){
			logger.info("output fileName=" + this.outputFileName + ",progress=" + percent + "%" );
			this.lastPercent = percent;
		}
		
		_handle(line);
	}
	
	protected abstract void _handle(String line) throws ETLException;
	
	protected void writeFiles(BufferedWriter output, Set<String> records,String fileName) {
		if (records.size() <= 0 || output == null) {
			return;
		}

		//int count = 0;
		StringBuffer sb = new StringBuffer();
		Iterator<String> iter = records.iterator();
		while (iter.hasNext()) {
			String line = iter.next();
			sb.append(line).append("\r\n");
			records.remove(line);
			//count++;
		}

		try {
			output.write(sb.toString());
			output.flush();
		} catch (IOException e) {
			logger.error("flush error file:" + e.getMessage());
		}

		//logger.info("write file:" + fileName + ",record count=" + count);
	}

	public void cleanup() throws ETLException {
		// write remaining error record
		this.writeFiles(this.queueOutStream, this.queue, this.queueFileName);
		this.writeFiles(this.errorOutStream, this.errorRecords,this.errorFileName);
		this.writeFiles(this.invalidOutStream, this.invalidRecords,this.invalidFileName);

		if (this.queueOutStream != null) {
			try {
				this.queueOutStream.close();
			} catch (IOException e) {
				throw new ETLException(e);
			}
		}

		if (this.errorOutStream != null) {
			try {
				this.errorOutStream.close();
			} catch (IOException e) {
				throw new ETLException(e);
			}
		}

		if (this.invalidOutStream != null) {
			try {
				this.invalidOutStream.close();
			} catch (IOException e) {
				throw new ETLException(e);
			}
		}
	}

	public void setInputFile(InputFile file) {
		this.inputFile = file;
	}

	public InputFile getInputFile() {
		return this.inputFile;
	}

	public long getRecordCount() {
		return this.recordCount;
	}

	public long getErrorCount() {
		return this.errorCount;
	}
	
	public String getOutputFileName() {
		return this.outputFileName;
	}

	public long getTaskSize(){
		return this.taskSize.longValue();
	}

	public void setTaskSize(long taskSize) {
		this.taskSize = new BigDecimal(taskSize);
	}
}
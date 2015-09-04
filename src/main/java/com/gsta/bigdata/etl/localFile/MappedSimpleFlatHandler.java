package com.gsta.bigdata.etl.localFile;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.OutputMetaData;
import com.gsta.bigdata.etl.core.process.LocalFileProcess;
import com.gsta.bigdata.etl.core.source.ValidatorException;

/**
 * @deprecated because the cost time is nearly by the BufferedWriter
 * @author tianxq
 *
 */
public class MappedSimpleFlatHandler extends AbstractHandler {
	private MappedFile errorMappedFile;
	private MappedFile invalidMappedFile;
	private MappedFile queueMappedFile;

	private Logger logger = LoggerFactory.getLogger(getClass());

	private int fileCount = 1;

	public MappedSimpleFlatHandler(LocalFileProcess process) {
		super(process);
	}

	@Override
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

		String fileName = this.inputFile.getFile().getName();
		logger.info(fileName);
		fileName = fileName.substring(0, fileName.lastIndexOf("."));

		String fileSuffix = "txt";
		OutputMetaData outputMetaData = this.process.getOutputMetaData();
		if (outputMetaData != null) {
			fileSuffix = outputMetaData.getFileSuffix();
			this.outputCharset = outputMetaData.getCharset();
		}
		
		this.errorMappedFile = new MappedFile(fileName,errorPath,"error");
		this.queueMappedFile = new MappedFile(fileName,outputPath,fileSuffix);
		this.invalidMappedFile = new MappedFile(fileName,errorPath,"invalid");
		
		this.errorMappedFile.boundChannelToByteBuffer();
		this.queueMappedFile.boundChannelToByteBuffer();
		this.invalidMappedFile.boundChannelToByteBuffer();

		String thresholdCount = process
				.getConf(Constants.CF_LOCAL_FILE_WRITE_COUNT);
		if (thresholdCount != null && !"".equals(thresholdCount)) {
			this.recordThreshold = Integer.parseInt(thresholdCount);
		}

		thresholdCount = process.getConf(Constants.CF_ERROR_RECORD_WRITE_COUNT);
		if (thresholdCount != null && !"".equals(thresholdCount)) {
			this.errorRecordThreshold = Integer.parseInt(thresholdCount);
		}

		this.recordCount = 0;
	}

	@Override
	public void _handle(String line) throws ETLException {
		ETLData data = null;

		try {
			data = this.process.parseLine(line, this.invalidRecords);
		} catch (ETLException e) {
			logger.error("dataline=" + line + ",error:" + e.getMessage());

			// write error file
			this.errorRecords.add(line);
			if (this.errorRecords.size() >= this.errorRecordThreshold) {
				writeFiles(this.errorMappedFile, this.errorRecords);
			}
			this.errorCount++;

			// if occuring parsing exception,write error file and don't make
			// transform
			return;
		} catch (ValidatorException e) {
			logger.error("dataline=" + line + ",invalid:" + e.getMessage());

			// write invalid file
			this.invalidRecords.add(line);
			if (this.invalidRecords.size() >= this.errorRecordThreshold) {
				writeFiles(this.invalidMappedFile, this.invalidRecords);
			}
			this.errorCount++;

			return;
		}

		try {
			if (null != data) {
				this.process.onTransform(data, SCOPE);

				String outputValue = this.process.getOutputValue(data);

				// write result file
				this.queue.add(outputValue);
				if (this.queue.size() >= this.recordThreshold) {
					writeFiles(this.queueMappedFile, this.queue);
				}

				this.recordCount++;
			}
		} catch (ETLException e) {
			logger.error("dataline=" + line + ",error:" + e.getMessage());

			// write error file
			this.errorRecords.add(line);
			if (this.errorRecords.size() >= this.errorRecordThreshold) {
				writeFiles(this.errorMappedFile, this.errorRecords);
			}
			this.errorCount++;
		}
	}

	private void writeFiles(MappedFile mappedFile, Set<String> records) {
		if (records.size() <= 0 || mappedFile == null) {
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

		try {
			byte[] data = sb.toString().getBytes(this.outputCharset);
			boolean flag = mappedFile.appendData(data);
			//write full file size
			if(!flag){
				mappedFile.flush();
				mappedFile.unmap();
				
				//write new file
				String fileName = mappedFile.getFileName() + this.fileCount;
				String fileDirPath = mappedFile.getFileDirPath();
				String fileSuffix = mappedFile.getFileSuffix();
				mappedFile = new MappedFile(fileName,fileDirPath,fileSuffix);
				mappedFile.appendData(data);
				
				this.fileCount++;
			}
		
		} catch (ETLException | UnsupportedEncodingException e) {
			logger.error("flush error file:" + e.getMessage());
		}

		logger.info("write file:" + mappedFile.getFileName() + ",record count=" + count);
	}

	@Override
	public void cleanup() throws ETLException {
		// write remaining error record
		this.writeFiles(this.queueMappedFile, this.queue);
		this.writeFiles(this.errorMappedFile, this.errorRecords);
		this.writeFiles(this.invalidMappedFile, this.invalidRecords);

		if (this.queueMappedFile != null) {
			this.queueMappedFile.flush();
			this.queueMappedFile.unmap();
		}

		if (this.errorMappedFile != null) {
			this.errorMappedFile.flush();
			this.errorMappedFile.unmap();
		}

		if (this.invalidMappedFile != null) {
			this.invalidMappedFile.flush();
			this.invalidMappedFile.unmap();
		}
	}
}

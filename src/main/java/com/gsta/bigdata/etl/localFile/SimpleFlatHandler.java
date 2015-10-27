package com.gsta.bigdata.etl.localFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.ETLProcess;
import com.gsta.bigdata.etl.core.source.ValidatorException;

/**
 * simple flag file handler,only deal single line from source file
 * 
 * @author tianxq
 * 
 */
public class SimpleFlatHandler extends AbstractHandler {
	private Logger logger = LoggerFactory.getLogger(getClass());

	public SimpleFlatHandler(ETLProcess process) {
		super(process);
	}
	
	
	@Override
	protected void _handle(String line) throws ETLException {
		ETLData data = null;

		try {
			data = super.process.parseLine(line, super.invalidRecords);
		} catch (ETLException e) {
			logger.error("dataline=" + line + ",error:" + e.getMessage());

			// write error file
			super.errorRecords.add(line);
			if (super.errorRecords.size() >= super.errorRecordThreshold) {
				writeFiles(super.errorOutStream, super.errorRecords,super.errorFileName);
			}
			super.errorCount++;

			// if occuring parsing exception,write error file and don't make
			// transform
			return;
		} catch (ValidatorException e) {
			logger.error("dataline=" + line + ",invalid:" + e.getMessage());

			// write invalid file
			super.invalidRecords.add(line);
			if (super.invalidRecords.size() >= super.errorRecordThreshold) {
				writeFiles(super.invalidOutStream, super.invalidRecords,super.invalidFileName);
			}
			super.errorCount++;

			return;
		}

		try {
			if (null != data) {
				super.process.onTransform(data, SCOPE);

				String outputValue = super.process.getOutputValue(data);

				// write result file
				super.queue.add(outputValue);
				if (super.queue.size() >= super.recordThreshold) {
					writeFiles(super.queueOutStream, super.queue,super.queueFileName);
				}

				super.recordCount++;
			}
		} catch (ETLException e) {
			logger.error("dataline=" + line + ",error:" + e.getMessage());

			// write error file
			super.errorRecords.add(line);
			if (super.errorRecords.size() >= super.errorRecordThreshold) {
				writeFiles(super.errorOutStream, super.errorRecords,super.errorFileName);
			}
			super.errorCount++;
		}
	}
}

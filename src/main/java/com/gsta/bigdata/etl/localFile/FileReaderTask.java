package com.gsta.bigdata.etl.localFile;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.CyclicBarrier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.etl.LocalFileRunner.InputFile;
import com.gsta.bigdata.utils.StringUtils;

/**
 * 
 * @author tianxq
 *
 */
public class FileReaderTask implements Runnable {
	//default vale is 1024*1024=1M
	protected int bufferSize;  
	protected long fileSize;
	private InputFile inputFile;
	
	protected RandomAccessFile randomAccessFile;
	private AbstractHandler handler;
	protected String sourceCharset;
	
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	//wait for thread finish
	protected CyclicBarrier barrier; 
	
	public FileReaderTask(int bufferSize,InputFile inputFile,
			AbstractHandler handler,String charset,
			CyclicBarrier barrier){
		this.bufferSize = bufferSize;
		
		try {
			this.inputFile = inputFile;
			this.randomAccessFile = new RandomAccessFile(inputFile.getFile(), "r");
			this.fileSize = inputFile.getFile().length();
			
			logger.info("file " + inputFile.getFile().getName() + " size=" + fileSize);
		} catch ( IOException e) {
			e.printStackTrace();
			logger.error(e.toString());
		}
		
		this.handler = handler;
		
		this.sourceCharset = charset;
		this.barrier = barrier;
	}

	@Override
	public void run() {
		try {
			byte[] readBuff = new byte[bufferSize];
			
			MappedByteBuffer mapBuffer = this.randomAccessFile.getChannel().map(MapMode.READ_ONLY, 0, fileSize);
			
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			
			for (int offset = 0; offset < fileSize; offset += bufferSize) {
				int readLength;
				if (offset + bufferSize <= fileSize) {
					readLength = bufferSize;
				} else {
					readLength = (int) (fileSize - offset);
				}
				
				//The position of this buffer is incremented by length. 
				mapBuffer.get(readBuff, 0, readLength);
				for (int i = 0; i < readLength; i++) {
					byte tmp = readBuff[i];
					if (tmp == '\n' || tmp == '\r') {
						this.handler.handle(StringUtils.byte2Str(bos.toByteArray(),this.sourceCharset));
						bos.reset();
					} else {
						bos.write(tmp);
					}
				}
			}
			
			//the remaining byte in bos buffer
			if (bos.size() > 0) {
				this.handler.handle(StringUtils.byte2Str(bos.toByteArray(),this.sourceCharset));
			}

			//await other thread finish
			barrier.await();
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.toString());
		}finally{
			if(this.randomAccessFile != null){
				try {
					this.randomAccessFile.close();
				} catch (IOException e) {
					e.printStackTrace();
					logger.error(e.toString());
				}
			}
		}
	}
	
	public void setBarrier(CyclicBarrier barrier) {
		this.barrier = barrier;
	}
	
	public InputFile getInputFile() {
		return inputFile;
	}
}
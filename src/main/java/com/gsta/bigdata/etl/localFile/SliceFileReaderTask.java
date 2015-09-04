package com.gsta.bigdata.etl.localFile;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.etl.LocalFileRunner.InputFile;

public class SliceFileReaderTask extends FileReaderTask {
	private int threadSize;
	private AbstractHandler[] handler;
	private Logger logger = LoggerFactory.getLogger(getClass());
	private Set<StartEndPair> startEndPairs;
	private ExecutorService executorService;

	public SliceFileReaderTask(int threadSize, int bufferSize, InputFile inputFile,String charset) {
		super(bufferSize,inputFile,null,charset,null);
		this.threadSize = threadSize;
		
		this.startEndPairs = new HashSet<StartEndPair>();
	}
	
	public int countRealSliceSize() throws IOException {
		long sliceSize = super.fileSize / this.threadSize;
		//if file is small and thread size is big enough,start at the first.
		if (sliceSize <= 0) {
			sliceSize = 1;
		}
		calculateStartEnd(0, sliceSize);
		
		return this.startEndPairs.size();
	}
	
	@Override
	public void run() {
		this.executorService = Executors.newFixedThreadPool(this.startEndPairs.size());
		
		int i = 0;
		for (StartEndPair pair : startEndPairs) {
			SliceReaderTask sliceReaderTask = new SliceReaderTask(
					super.bufferSize, handler[i], pair, super.barrier,
					super.randomAccessFile, super.sourceCharset);
			
			this.executorService.execute(sliceReaderTask);
			i++;
		}
		
		this.executorService.shutdown();
		try {
			super.barrier.await();
		} catch (InterruptedException | BrokenBarrierException e) {
			e.printStackTrace();
		}
		
		try {
			super.randomAccessFile.close();
		} catch (IOException e) {
			e.printStackTrace();
			logger.error(e.toString());
		}
	}

	private void calculateStartEnd(long start, long size) throws IOException {
		if (start > super.fileSize - 1) {
			return;
		}

		StartEndPair pair = new StartEndPair();
		pair.setStart(start);
		long endPosition = start + size - 1;
		if (endPosition >= super.fileSize - 1) {
			pair.setEnd(super.fileSize - 1);
			this.startEndPairs.add(pair);
			return;
		}

		super.randomAccessFile.seek(endPosition);
		byte tmp = (byte) super.randomAccessFile.read();
		while (tmp != '\n' && tmp != '\r') {
			endPosition++;
			if (endPosition >= super.fileSize - 1) {
				endPosition = super.fileSize - 1;
				break;
			}
			super.randomAccessFile.seek(endPosition);
			tmp = (byte) super.randomAccessFile.read();
		}
		pair.setEnd(endPosition);
		startEndPairs.add(pair);

		calculateStartEnd(endPosition + 1, size);
	}

	public int getRealSliceSize(){
		return this.startEndPairs.size();
	}
	
	public void setHandler(AbstractHandler[] handler) {
		this.handler = handler;
	}

	// every slice start and end position
	protected static class StartEndPair {
		private long start;
		private long end;

		public long getStart() {
			return start;
		}

		public void setStart(long start) {
			this.start = start;
		}

		public long getEnd() {
			return end;
		}

		public void setEnd(long end) {
			this.end = end;
		}
	}
}

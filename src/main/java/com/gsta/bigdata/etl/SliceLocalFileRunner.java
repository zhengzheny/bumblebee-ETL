package com.gsta.bigdata.etl;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ETLProcess;
import com.gsta.bigdata.etl.localFile.HandlerFactory;
import com.gsta.bigdata.etl.localFile.AbstractHandler;
import com.gsta.bigdata.etl.localFile.SliceFileReaderTask;

/**
 * slice local file runner,every file has a thread. further on,there are
 * threadSize thread which is defined by computingFramework to deal the file.
 * 
 * @author tianxq
 *
 */
public class SliceLocalFileRunner extends LocalFileRunner {
	private Logger logger = LoggerFactory.getLogger(getClass());

	public SliceLocalFileRunner(ETLProcess process) {
		super(process);
	}

	@Override
	public int etlRun() throws ETLException {
		logger.info("etl process=" + super.process.getId() + " begin...");
		long beginTime = System.currentTimeMillis();
		super.setup();

		if (super.files.size() <= 0) {
			logger.error("there is no qualified files to deal.");
			return -1;
		}

		// initialize thread pool,every file has one thread
		int filesCount = super.files.size();
		String strThreadSize = super.process.getConf(Constants.CF_LOCAL_FILE_SLICE_THREAD_SIZE, "100");
		int threadSize = Integer.parseInt(strThreadSize);
		logger.info("thread size=" + threadSize);

		SliceFileReaderTask[] sliceFileReaderTasks = new SliceFileReaderTask[filesCount];

		int i = 0;
		int realTotalThreadCount = 0;
		Iterator<InputFile> iter = super.files.iterator();
		while (iter.hasNext()) {
			InputFile inputFile = iter.next();

			sliceFileReaderTasks[i] = new SliceFileReaderTask(threadSize,
					super.bufferSize, inputFile, inputFile.getCharset());
			try {
				int sliceSize = sliceFileReaderTasks[i].countRealSliceSize();
				realTotalThreadCount += sliceSize;
				long fileSize = inputFile.getFile().length();
				logger.info("source file=" + inputFile.getFile().getName()
						+ ",fileSize=" + fileSize + ",slice size= " + sliceSize);
			} catch (IOException e) {
				throw new ETLException(e);
			}
			i++;
		}

		ExecutorService executorService = Executors.newFixedThreadPool(filesCount);
		// cyclic barrier include the main thread
		CyclicBarrier barrier = new CyclicBarrier(realTotalThreadCount + filesCount + 1);
		AbstractHandler[] handlers = new AbstractHandler[realTotalThreadCount];
		
		int pos = 0;
		for (i = 0; i < filesCount; i++) {
			int sliceSize = sliceFileReaderTasks[i].getRealSliceSize();
			for (int j = 0; j < sliceSize; j++) {
				int index = pos + j;
				handlers[index] = HandlerFactory.createHandler(process);
				handlers[index].setInputFile(sliceFileReaderTasks[i].getInputFile());
				handlers[index].setOuputFileName(j);
				handlers[index].setup();
			}

			AbstractHandler[] fileHandlers = new AbstractHandler[sliceSize];
			System.arraycopy(handlers, pos, fileHandlers, 0, sliceSize);
			
			pos = pos + sliceSize;

			sliceFileReaderTasks[i].setBarrier(barrier);
			sliceFileReaderTasks[i].setHandler(fileHandlers);
			
			executorService.execute(sliceFileReaderTasks[i]);
		}

		// stop thread pool,waiting all thread finish and clean up handler
		executorService.shutdown();
		try {
			barrier.await();
		} catch (InterruptedException | BrokenBarrierException e) {
			throw new ETLException(e);
		}

		logger.info("begin clean up handler...");
		for (i = 0; i < realTotalThreadCount; i++) {
			handlers[i].cleanup();
		}

		// write file list and print report
		super.writeFileList(handlers);

		long endTime = System.currentTimeMillis();
		super.printReport(beginTime, endTime, handlers);

		return 0;
	}

}

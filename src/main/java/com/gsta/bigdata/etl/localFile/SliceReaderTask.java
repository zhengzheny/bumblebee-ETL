package com.gsta.bigdata.etl.localFile;

import java.io.ByteArrayOutputStream;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.CyclicBarrier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.utils.StringUtils;

/**
 * slice reader task from file
 * @author tianxq
 *
 */
public class SliceReaderTask implements Runnable {
	private long start;
	private long end;
	private long sliceSize;
	private int bufferSize;
	private AbstractHandler handler;
	private CyclicBarrier barrier;
	private RandomAccessFile randomAccessFile;
	private String sourceCharset;
	
	private Logger logger = LoggerFactory.getLogger(getClass());

	public SliceReaderTask(int bufferSize, AbstractHandler handler,
			SliceFileReaderTask.StartEndPair startEndPair,
			CyclicBarrier barrier, RandomAccessFile randomAccessFile,
			String sourceCharset) {
		this.bufferSize = bufferSize;
		this.handler = handler;
		this.barrier = barrier;

		this.start = startEndPair.getStart();
		this.end = startEndPair.getEnd();
		this.sliceSize = startEndPair.getEnd() - startEndPair.getStart() + 1;
		handler.setTaskSize(sliceSize);

		this.randomAccessFile = randomAccessFile;
		this.sourceCharset = sourceCharset;
	}

	@Override
	public void run() {
		try {
			logger.info("fileName " + handler.getInputFile().getFile().getName() +
					" slice:start=" + this.start + ",end=" + this.end +
					",sliceSize=" + this.sliceSize);
			byte[] readBuff = new byte[bufferSize];

			MappedByteBuffer mapBuffer = this.randomAccessFile.getChannel()
					.map(MapMode.READ_ONLY, this.start, this.sliceSize);

			ByteArrayOutputStream bos = new ByteArrayOutputStream();

			for (int offset = 0; offset < this.sliceSize; offset += bufferSize) {
				int readLength;
				if (offset + bufferSize <= this.sliceSize) {
					readLength = bufferSize;
				} else {
					readLength = (int) (this.sliceSize - offset);
				}

				// The position of this buffer is incremented by length.
				mapBuffer.get(readBuff, 0, readLength);
				for (int i = 0; i < readLength; i++) {
					byte tmp = readBuff[i];
					if (tmp == '\n' || tmp == '\r') {
						this.handler.handle(StringUtils.byte2Str(
								bos.toByteArray(), this.sourceCharset));
						bos.reset();
					} else {
						bos.write(tmp);
					}
				}
			}

			// the remaining byte in bos buffer
			if (bos.size() > 0) {
				this.handler.handle(StringUtils.byte2Str(bos.toByteArray(),
						this.sourceCharset));
			}

			// await other thread finish
			this.barrier.await();
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.toString());
		}
	}
}

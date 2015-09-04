package com.gsta.bigdata.etl.localFile;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.etl.ETLException;
 
/**
 * @deprecated
 * @author tianxq
 *
 */
public class MappedFile {
    private File file;
 
    private MappedByteBuffer mappedByteBuffer;
    private FileChannel fileChannel;
    private boolean boundSuccess = false;
 
    //the max file size is 2G
    private final static long MAX_FILE_SIZE = 1024 * 1024 * 2000; 
    //the max dirty data is 1M and force flush
    private long MAX_FLUSH_DATA_SIZE = 1024 * 1024;
    //the max time gap 1s and force flush
    private long MAX_FLUSH_TIME_GAP = 1000;
    
    private long writePosition = 0;
    private long lastFlushTime;
    private long lastFlushFilePosition = 0;
    
    private String fileName;
    private String fileDirPath;
    private String fileSuffix;
    
    private Logger logger = LoggerFactory.getLogger(getClass());
     
    public MappedFile(String fileName, String fileDirPath,String fileSuffix) {
    	this.fileName = fileName;
    	this.fileDirPath = fileDirPath;
    	this.fileSuffix = fileSuffix;
    	
    	String temp = this.fileDirPath + "/" + this.fileName + "." + this.fileSuffix;
    	
        this.file = new File(temp);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
                logger.error(e.toString());
            }
        }
         
    }
 
    public synchronized boolean boundChannelToByteBuffer() throws ETLException {
        try {
            @SuppressWarnings("resource")
			RandomAccessFile raf = new RandomAccessFile(file, "rw");
            this.fileChannel = raf.getChannel();
        } catch (Exception e) {
            this.boundSuccess = false;
            throw new ETLException(e);
        }
 
        try {
            this.mappedByteBuffer = this.fileChannel
                    .map(FileChannel.MapMode.READ_WRITE, 0, MAX_FILE_SIZE);
        } catch (IOException e) {
            this.boundSuccess = false;
            throw new ETLException(e);
        }
 
        this.boundSuccess = true;
        return true;
    }
    
    public synchronized boolean appendData(byte[] data) throws ETLException {
        if (!boundSuccess) {
            boundChannelToByteBuffer();
        }
         
        writePosition = writePosition + data.length;
        if (writePosition >= MAX_FILE_SIZE) {   
            flush();
            writePosition = writePosition - data.length;
            logger.info("File="  + file.toURI().toString() + " is written full.");
            logger.info("already write data length:" + writePosition
                                + ", max file size=" + MAX_FILE_SIZE);
            return false;
        }
 
        this.mappedByteBuffer.put(data);
 
        //check flush disk
		long deltaPosition = writePosition - lastFlushFilePosition;
		long deltaTimeGap = System.currentTimeMillis() - lastFlushTime;
		if ((deltaPosition > this.MAX_FLUSH_DATA_SIZE)
				|| (deltaTimeGap > this.MAX_FLUSH_TIME_GAP && writePosition > lastFlushFilePosition)) {
			flush();
		}
         
        return true;
    }
 
    public synchronized void flush() {
        this.mappedByteBuffer.force();
        this.lastFlushTime = System.currentTimeMillis();
        this.lastFlushFilePosition = writePosition;
    }
    
    /**
     * unmap direct buffer,after force map,if there is read map buffer,
     * it will occur jvm crash,so check reading or writing thread. 
     */
	public void unmap() {
		if (this.mappedByteBuffer == null) {
			return;
		}
		
		try {
			this.mappedByteBuffer.force();
			AccessController.doPrivileged(new PrivilegedAction<Object>() {
				@Override
				@SuppressWarnings("restriction")
				public Object run() {
					try {
						Method getCleanerMethod = mappedByteBuffer.getClass()
								.getMethod("cleaner", new Class[0]);
						getCleanerMethod.setAccessible(true);
						sun.misc.Cleaner cleaner = (sun.misc.Cleaner) getCleanerMethod
								.invoke(mappedByteBuffer, new Object[0]);
						cleaner.clean();

					} catch (Exception e) {
						e.printStackTrace();
						logger.error(e.toString());
					}
					
					logger.info("clean MappedByteBuffer completed");
					return null;
				}
			});

		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.toString());
		}
	}

	public String getFileName() {
		return fileName + "." + fileSuffix;
	}

	public String getFileDirPath() {
		return fileDirPath;
	}

	public String getFileSuffix() {
		return fileSuffix;
	}
}

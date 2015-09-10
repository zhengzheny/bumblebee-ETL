package com.gsta.bigdata.etl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ShellContext;
import com.gsta.bigdata.etl.core.process.LocalFileProcess;
import com.gsta.bigdata.etl.core.source.InputPath;
import com.gsta.bigdata.etl.localFile.FileFilterFactory;
import com.gsta.bigdata.etl.localFile.FileReaderTask;
import com.gsta.bigdata.etl.localFile.HandlerFactory;
import com.gsta.bigdata.etl.localFile.AbstractHandler;

/**
 * local file computing framework runner
 * one thread to deal one input file
 * 
 * @author tianxq
 * 
 */
public class LocalFileRunner implements IRunner {
	protected LocalFileProcess process;
	protected List<InputFile> files = new ArrayList<InputFile>();
	// read source file buffer size,the default value is 1M
	protected int bufferSize = 1024 * 1024;
	private Logger logger = LoggerFactory.getLogger(getClass());

	public LocalFileRunner(LocalFileProcess process) {
		this.process = process;
	}

	protected void setup() throws ETLException {
		if (this.process == null) {
			throw new ETLException("process is null");
		}

		// this runner can only deal max file,default value is 1000
		String strMaxFile = this.process.getConf(Constants.CF_LOCAL_FILE_DIR_MAX_FILE, "1000");
		int maxFile = Integer.parseInt(strMaxFile);

		// if restart from shell context is 1,don't verify done file list file.
		String strReStart = ShellContext.getInstance().getValue(Constants.SHELL_CTX_RESTART, "0");
		boolean restart = false;
		if ("1".equals(strReStart)) {
			restart = true;
		}

		/*
		 * get input files,if inputPath is directory, list all files according
		 * file suffix and file name pattern
		 */
		List<InputPath> inputPaths = this.process.getInputPaths();
		Iterator<InputPath> iter = inputPaths.iterator();
		while (iter.hasNext()) {
			InputPath inputPath = iter.next();
			String fileName = inputPath.getPath();
			File file = new File(fileName);

			if (!file.exists()) {
				throw new ETLException("file " + fileName + " doesn't exists.");
			}

			if (file.isDirectory()) {
				// filter by data source definition
				FilenameFilter ff = FileFilterFactory.createFileFilter(
						this.process, inputPath.getFileSuffix(),
						inputPath.getFileNamePattern());
				File[] subFiles = file.listFiles(ff);
				String dirPath = file.getPath();

				if (subFiles.length > maxFile) {
					logger.info("dir:" + file.getPath() + " have "
							+ subFiles.length
							+ " files,this process will only deal " + maxFile
							+ " files and remember file list,you will run "
							+ " for dealing other files after this process.");
				}

				Set<String> fileList = this.loadDoneFile(fileName);

				int count = 0;
				for (File tempFile : subFiles) {
					//ignore sub file directory
					if(tempFile.isDirectory()){
						continue;
					}
					
					if (count >= maxFile) {
						break;
					}

					// filter by done file list
					if (!restart && fileList.contains(tempFile.getName())) {
						continue;
					}

					InputFile inputFile = new InputFile(tempFile,inputPath.getCharset(), true, dirPath);
					this.files.add(inputFile);

					count++;
				}
			} else {
				InputFile inputFile = new InputFile(file,inputPath.getCharset(), false, null);
				this.files.add(inputFile);
			}
		}//end while

		// get buffer size from computing framework
		String strBufferSize = this.process.getConf(Constants.CF_LOCAL_FILE_BUFFER_SIZE);
		if (strBufferSize != null && !"".equals(strBufferSize)) {
			this.bufferSize = Integer.parseInt(strBufferSize);
		}
		logger.info("buffer size=" + this.bufferSize);
	}

	@Override
	public int etlRun() throws ETLException {
		logger.info("etl process=" + this.process.getId() + " begin...");
		long beginTime = System.currentTimeMillis();
		this.setup();

		if (this.files.size() <= 0) {
			logger.error("there is no qualified files to deal.");
			return -1;
		}

		// initialize thread pool,every file has one thread
		int filesCount = this.files.size();
		logger.info("thread size is " + filesCount);

		ExecutorService executorService = Executors.newFixedThreadPool(filesCount);
		AbstractHandler[] handlers = new AbstractHandler[filesCount];
		// cyclic barrier count must have threadSize+1,
		// because include the main thread
		CyclicBarrier barrier = new CyclicBarrier(filesCount + 1);

		int i = 0;
		Iterator<InputFile> iter = this.files.iterator();
		while (iter.hasNext()) {
			InputFile inputFile = iter.next();

			handlers[i] = HandlerFactory.createHandler(process);
			handlers[i].setInputFile(inputFile);
			handlers[i].setOuputFileName(-1);
			handlers[i].setTaskSize(inputFile.getFile().length());
			handlers[i].setup();

			FileReaderTask fileReaderTask = new FileReaderTask(this.bufferSize,
					inputFile, handlers[i], inputFile.getCharset(),barrier);

			executorService.execute(fileReaderTask);
			i++;
		}

		// stop thread pool,waiting all thread finish and clean up handler
		executorService.shutdown();
		try {
			barrier.await();
		} catch (InterruptedException | BrokenBarrierException e) {
			throw new ETLException(e);
		}

		logger.info("begin clean up handler...");
		for (i = 0; i < filesCount; i++) {
			handlers[i].cleanup();
		}

		// write file list and print report
		this.writeFileList(handlers);

		long endTime = System.currentTimeMillis();
		this.printReport(beginTime, endTime, handlers);

		return 0;
	}

	protected void printReport(long beginTime, long endTime, AbstractHandler[] handlers) {
		logger.info("****************etl report******************");
		
		String info = "";
		// total files,total record count
		// every file name,record count
		long totalRecoudCount = 0;
		long totalErrorCount = 0;
		long totalFileSize = 0;
				
		for (AbstractHandler handler : handlers) {
			info = "file=" + handler.getOutputFileName()
					+ ",size=" + convertFileSize(handler.getTaskSize())
					+ ",record count=" + handler.getRecordCount();
			if (handler.getErrorCount() > 0) {
				info = info + ",error count=" + handler.getErrorCount();
			}
			logger.info(info);
			
			totalRecoudCount += handler.getRecordCount();
			totalErrorCount += handler.getErrorCount();
			totalFileSize += handler.getTaskSize();
		}
		
		info = "total files=" + handlers.length + ",total size="
				+ convertFileSize(totalFileSize) + ",total record count="
				+ totalRecoudCount;
		if (totalErrorCount > 0) {
			info = info + ",total error count = " + totalErrorCount;
		}
		logger.info(info);

		float costTime = (float) ((endTime - beginTime) / 1000.0);
		logger.info("etl process=" + this.process.getId() + " cost time:"
				+ Math.round(costTime) + " seconds");
	}
	
	private String convertFileSize(long size) {
		long kb = 1024;
		long mb = 1024 * kb;
		long gb = 1024 * mb;
		
		if(size >= gb){
			return String.format("%.2f GB",(float)size / gb);
		}else if(size >= mb){
			float f = (float) size / mb;
			return String.format("%.2f MB", f);
		}else if(size >= kb){
			float f = (float) size / kb;
			return String.format("%.2f KB", f);
		}else{
			return String.format("%d B", size);
		}
	}

	/**
	 * the file name is like path1.path2.path3
	 * 
	 * @param inputPath
	 * @return
	 */
	private String getFileNameByInputPath(String inputPath) {
		String ret = null;

		if (inputPath != null) {
			ret = inputPath;
			ret = ret.replace('/', '.');
			ret = ret.replace('\\', '.');
			// only for test in windows OS,ex:D:.bigdata.gdnoc-etl
			ret = ret.substring(ret.indexOf(":") + 1);
			//delete first dot,because in Linux OS,it's hidden file
			ret = ret.substring(ret.indexOf(".") + 1);
		}

		return ret;
	}

	//save result directory
	private String getResultDir() {
		return System.getProperty("user.dir") + File.separator + "result/";
	}

	/**
	 * if defined input path is directory in configure xml file, writing handled
	 * file to file list. if file count is more than localFileDirMaxFile in
	 * computingFrameworkConfigs which is defined in configure xml file,user
	 * should run more again to deal the remaining file according file list. if
	 * the file is in file list,ETL program doesn't deal it.
	 * 
	 */
	protected void writeFileList(AbstractHandler[] handlers) {
		String resultDir = this.getResultDir();

		File dir = new File(resultDir);
		if (!dir.exists()) {
			dir.mkdirs();
		}

		if (handlers == null || handlers.length == 0) {
			logger.error("handlers is null.");
			return;
		}

		// get handler file list
		Map<String, Set<String>> results = new HashMap<String, Set<String>>();
		for (AbstractHandler handler : handlers) {
			InputFile inputFile = handler.getInputFile();
			// only write inputPaht is directory
			if (inputFile.isDir()) {
				String inputPath = this.getFileNameByInputPath(inputFile.getDirPath());
				Set<String> fileList = results.get(inputPath);
				if (fileList == null) {
					fileList = new HashSet<String>();
					fileList.add(inputFile.getFile().getName());
					results.put(inputPath, fileList);
				} else {
					fileList.add(inputFile.getFile().getName());
				}
			}// end if
		}// end for

		DateFormat formatFrom = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String strDate = formatFrom.format(new Date());

		// write file list to file
		Set<String> keys = results.keySet();
		Iterator<String> iter = keys.iterator();
		while (iter.hasNext()) {
			String header = "# processId=" + this.process.getId() + " " + strDate;
			String inputDir = iter.next();
			String fileName = resultDir + inputDir;
			logger.info("write result file=" + fileName);

			FileWriter fileWriter = null;
			try {
				fileWriter = new FileWriter(fileName, true);
				fileWriter.write(header);
				fileWriter.write("\r\n");

				Set<String> fileList = results.get(inputDir);
				Iterator<String> fileListIter = fileList.iterator();
				while (fileListIter.hasNext()) {
					fileWriter.write(fileListIter.next());
					fileWriter.write("\r\n");
				}
			} catch (IOException e) {
				e.printStackTrace();
				logger.error(e.toString());
			} finally {
				try {
					if (fileWriter != null) {
						fileWriter.close();
					}
				} catch (IOException e) {
					e.printStackTrace();
					logger.error(e.toString());
				}
			}
		}// end while key
	}

	private Set<String> loadDoneFile(String dir) {
		Set<String> ret = new HashSet<String>();

		String fileName = this.getResultDir() + this.getFileNameByInputPath(dir);
		File file = new File(fileName);
		if (!file.exists()) {
			return ret;
		}

		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			String line = null;
			while ((line = reader.readLine()) != null) {
				if (!line.startsWith("#")) {
					ret.add(line.trim());
				}
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
			logger.error(e.toString());
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
					logger.error(e.toString());
				}
			}
		}

		return ret;
	}

	public class InputFile {
		private File file;
		private String charset;
		private boolean isDir = false;
		private String dirPath;

		public InputFile(File file, String charset, boolean isDir,
				String dirPath) {
			this.file = file;
			this.charset = charset;
			this.isDir = isDir;
			this.dirPath = dirPath;
		}

		public File getFile() {
			return file;
		}

		public String getCharset() {
			return charset;
		}

		public boolean isDir() {
			return isDir;
		}

		public String getDirPath() {
			return dirPath;
		}
	}
}

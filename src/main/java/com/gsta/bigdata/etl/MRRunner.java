package com.gsta.bigdata.etl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.WriteLog;
import com.gsta.bigdata.etl.core.lookup.LKPTableMgr;
import com.gsta.bigdata.etl.core.process.MRProcess;
import com.gsta.bigdata.etl.core.source.InputPath;
import com.gsta.bigdata.etl.mapreduce.ErrorCodeCount;
import com.gsta.bigdata.utils.BeansUtils;
import com.gsta.bigdata.utils.HdfsUtils;
import com.gsta.bigdata.utils.JDBCUtils;

/**
 * MR computing framework runner
 * 
 * @author tianxq
 * 
 */
public class MRRunner extends Configured implements Tool, IRunner {
	private Logger logger = LoggerFactory.getLogger(getClass());
	private MRProcess process;
	private LKPTableMgr lkpTableMgr;
	//write map/reduce result to database <writeLog property="conf/log/dblog.properties" />
	private WriteLog writeLog;
	private Configuration conf;
	
	private static final String HDFS_PRE = "hdfs://";
	private static final String ERROR_INFO_FILE_PREFIX = "errorInfo";

	public MRRunner(Configuration conf, MRProcess process,
			LKPTableMgr lkpTableMgr, WriteLog writeLog) {
		super(conf);

		this.conf = conf;
		this.process = process;
		this.lkpTableMgr = lkpTableMgr;
		this.writeLog = writeLog;
	}

	@Override
	public int etlRun() throws ETLException {
		try {
			return ToolRunner.run(this.conf, this, null);
		} catch (Exception e) {
			throw new ETLException(e);
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		// process class to json
		if (null == process) {
			throw new Exception("MRProcess instance is null.");
		}

		conf.set(Constants.HADOOP_CONF_MRPROCESS,
				BeansUtils.obj2json(this.process));
		conf.set(Constants.PATH_LOOKUP, BeansUtils.obj2json(this.lkpTableMgr));

		Date startTime = new Date();
		Job job = Job.getInstance(conf, this.process.getId());
		job.setJarByClass(getClass());

		String mapperClass = process.getConf(Constants.HADOOP_MAPPER_CLASS,
				Constants.HADOOP_DEFAULT_MAPPER_CLASS);
		job.setMapperClass(this.loadClass(mapperClass));

		String reducerClass = process.getConf(Constants.HADOOP_REDUCER_CLASS);
		if (null != reducerClass && !"".equals(reducerClass)) {
			job.setReducerClass(this.loadClass(reducerClass));
		}

		String combinerClass = process.getConf(Constants.HADOOP_COMBINER_CLASS);
		if (null != combinerClass && !"".equals(combinerClass)) {
			job.setCombinerClass(this.loadClass(combinerClass));
		}

		String inputFormatClass = process.getConf(
				Constants.HADOOP_INPUTFORMAT_CLASS,
				Constants.HADOOP_DEFAULT_INPUTFORMAT_CLASS);
		job.setInputFormatClass(this.loadClass(inputFormatClass));

		String outputFormatClass = process.getConf(
				Constants.HADOOP_OUTPUTFORMAT_CLASS,
				Constants.HADOOP_DEFAULT_OUTPUTFORMAT_CLASS);
		job.setOutputFormatClass(this.loadClass(outputFormatClass));

		String outputKeyClass = process.getConf(
				Constants.HADOOP_OUTPUTKEY_CLASS,
				Constants.HADOOP_IO_TEXT_CLASS);
		job.setOutputKeyClass(this.loadClass(outputKeyClass));

		String outputValueClass = process.getConf(
				Constants.HADOOP_OUTPUTVALUE_CLASS,
				Constants.HADOOP_IO_TEXT_CLASS);
		job.setOutputValueClass(this.loadClass(outputValueClass));

		List<InputPath> inputPaths = process.getInputPaths();
		Iterator<InputPath> iter = inputPaths.iterator();
		while (iter.hasNext()) {
			InputPath inputPath = iter.next();
			FileInputFormat.addInputPath(job, new Path(inputPath.getPath()));
		}

		// if output directory exists,delete it
		this.rmrDir(process.getOutputPath(), conf);
		// if error directory exists,delete it
		this.rmrDir(process.getErrorPath(), conf);
		FileOutputFormat.setOutputPath(job, new Path(process.getOutputPath()));

		int complete = job.waitForCompletion(true) ? 0 : 1;

		Date endTime = new Date();
		this.printErrorInfo2console(process, conf);
		this.printJobInfo(job, startTime, endTime);

		String property = this.writeLog.getProperty();
		if (property != null && !"".equals(property)) {
			this.recordToDatabase(property, conf, job, startTime, endTime);
		}

		System.exit(complete);
		return 0;
	}

	/**
	 * sometimes operation and maintenance staffs has no right
	 * to look up the maper or reducer log,so print the log to console
	 * @param process
	 * @param conf
	 * @throws IOException
	 */
	private void printErrorInfo2console(MRProcess process, Configuration conf)
			throws IOException {
		String outputPath = process.getOutputPath();
		FileSystem hdfs = FileSystem.get(URI.create(outputPath), conf);
		FileStatus[] fs = hdfs.listStatus(new Path(outputPath));
		Path[] listPath = FileUtil.stat2Paths(fs);
		logger.info("==============error information===========");
		Map<String, ErrorCodeCount> errorInfos = new HashMap<String, ErrorCodeCount>();
		
		for (Path p : listPath) {
			if (p.getName().startsWith(ERROR_INFO_FILE_PREFIX)) {
				String errorInformPath = outputPath + "/" + p.getName();
				HdfsUtils hdfsUtils = new HdfsUtils();
				InputStream in = hdfsUtils.load(errorInformPath);
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(in, Constants.DEFAULT_ENCODING));
				String line = "";
				while ((line = reader.readLine()) != null) {
					ErrorCodeCount errorCodeCount = BeansUtils.json2obj(line,
							ErrorCodeCount.class);
					String errorCode = errorCodeCount.getErrorCode();
					if(errorInfos.containsKey(errorCode)){
						errorInfos.get(errorCode).addCount(errorCodeCount.getCount());
					}else{
						errorInfos.put(errorCode, errorCodeCount);
					}
				}

				IOUtils.closeStream(reader);
				// delete error information file
				if (hdfs.exists(p) && hdfs.delete(p, true)) {
					logger.info("delete temp file=" + errorInformPath);
				}
			}
		}//end for
		
		for(ErrorCodeCount errorCodeCount:errorInfos.values()){
			logger.error(errorCodeCount.toString());
		}

		hdfs.close();
	}

	@SuppressWarnings("rawtypes")
	public Class loadClass(String className) throws ClassNotFoundException {
		return Class.forName(className);
	}

	// remove dir
	private void rmrDir(String delPath, Configuration conf) throws Exception {
		if (null == delPath || "".equals(delPath)) {
			throw new Exception("path dir is null");
		}

		String hdfsPath = "";
		String folder = "";
		if (delPath.startsWith(HDFS_PRE)) {
			hdfsPath = delPath.substring(0, delPath.indexOf('/', 8));
			folder = delPath.substring(delPath.indexOf('/', 8));
		} else {
			hdfsPath = delPath;
			folder = delPath;
		}

		Path path = new Path(folder);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		logger.info("path=" + path);

		if (fs.exists(path) && fs.delete(path, true)) {
			logger.info("delete  dir=" + folder);
		}

		fs.close();
	}

	// print job information
	private void printJobInfo(Job job, Date startTime, Date endTime) {
		logger.info("================job information===========");
		try {
			logger.info("job name: " + job.getJobName());
			logger.info("job success:" + (job.isSuccessful() ? "yes" : "no"));

			long totalLines = job.getCounters()
					.findCounter("org.apache.hadoop.mapred.Task$Counter",
							"MAP_INPUT_RECORDS").getValue();
			logger.info("total lines:" + totalLines);

			long outputLines = job.getCounters()
					.findCounter("org.apache.hadoop.mapred.Task$Counter",
							"REDUCE_OUTPUT_RECORDS").getValue();
			logger.info("output lines: " + outputLines);

			long errorLines = totalLines - outputLines;
			logger.info("error lines: " + errorLines);

			logger.info("begin time: " + formatDate(startTime));
			logger.info("end time: " + formatDate(endTime));
			float costTime = (float) ((endTime.getTime() - startTime.getTime()) / 1000.0);
			logger.info("cost time: " + Math.round(costTime) + " seconds");
		} catch (IOException e) {
			e.printStackTrace();
			logger.error(e.toString());
		}
	}

	// record run information to database
	private void recordToDatabase(String property, Configuration conf, Job job,
			Date startTime, Date endTime) {
		try {
			Map<String, Object> records = new HashMap<String, Object>();

			records.put(Constants.LOG_RECORD_TABLE_NAME, process.getId());
			String statDate = conf.get(Constants.LOG_RECORD_STAT_DATE);
			// statDate = statDate.substring(statDate.indexOf("=") + 1);
			records.put(Constants.LOG_RECORD_STAT_DATE, statDate);
			long totalLines = job.getCounters()
					.findCounter("org.apache.hadoop.mapred.Task$Counter",
							"MAP_INPUT_RECORDS").getValue();
			records.put(Constants.LOG_RECORD_TOTAL_NUMS, totalLines);
			long outputLines = job.getCounters()
					.findCounter("org.apache.hadoop.mapred.Task$Counter",
							"REDUCE_OUTPUT_RECORDS").getValue();
			records.put(Constants.LOG_RECORD_SUCCESS_NUMS, outputLines);
			long errorLines = totalLines - outputLines;
			records.put(Constants.LOG_RECORD_FAIL_NUMS, errorLines);
			records.put(Constants.LOG_RECORD_END_TIME, endTime);
			records.put(Constants.LOG_RECORD_COST_TIME,
					(endTime.getTime() - startTime.getTime()));
			records.put(Constants.LOG_RECORD_ERROR_PATH, process.getErrorPath());
			
			JDBCUtils.recordLog(property, records);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	private String formatDate(Date date) {
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return formatter.format(date);
	}
}

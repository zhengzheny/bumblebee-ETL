package com.gsta.bigdata.etl;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ContextProperty;
import com.gsta.bigdata.etl.core.ShellContext;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.etl.core.WriteLog;
import com.gsta.bigdata.etl.core.lookup.LKPTableMgr;
import com.gsta.bigdata.etl.core.lookup.LookupMgr;
import com.gsta.bigdata.etl.core.process.AbstractProcess;
import com.gsta.bigdata.etl.core.process.LocalFileProcess;
import com.gsta.bigdata.etl.core.process.MRProcess;
import com.gsta.bigdata.utils.FileUtils;
import com.gsta.bigdata.utils.XmlTools;

/**
 * etl main entry
 * 
 * @author tianxq
 * 
 */
public class ETLRunner {
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	public ETLRunner() {
	}

	/**
	 * get process object according xml configure file 
	 * @param configFile
	 * @param processId
	 * @return
	 */
	private Element getProcessNode(String configFile, String processId) {
		Preconditions.checkNotNull(configFile, "configFile is null.");

		InputStream inputStream = null;
		try {
			inputStream = FileUtils.getInputFile(configFile);
			if (inputStream == null) {
				logger.error("get null config file.");
				return null;
			}

			Document document = XmlTools.loadFromInputStream(inputStream);
			
			// if don't input processId,find first process element
			if (null == processId || "".equals(processId)) {
				Node node = XmlTools.getFirstChildByTagName(
						document.getDocumentElement(), Constants.PATH_PROCESS);
				if (null != node && node.getNodeType() == Node.ELEMENT_NODE) {
					return (Element) node;
				}
			}

			String processPath = "/etl/process[@id='" + processId + "']";
			Node processNode = XmlTools.getNodeByPath(document, processPath);
			if (processNode != null
					&& processNode.getNodeType() == Node.ELEMENT_NODE) {
				return (Element) processNode;
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.toString());
		} finally {
			if (null != inputStream) {
				try {
					inputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
					logger.error(e.toString());
				}
			}
		}

		return null;
	}

	protected void printUsage() {
		String usage = "Usage: "
				+ getClass().getSimpleName()
				+ " configFile=fileName processId=id paraN=valueN\r\n"
				+ "configFile - etl configuer file,exists in local file or classpath.\r\n"
				+ "processId - if no process id,the default is the first process in config file.\r\n"
				+ "paraN - as context for transform";

		System.out.println(usage);
	}

	public static void main(String[] args) {
		// usage:ETLRunner configFile=/etl.xml processId=ex1
		ETLRunner etlRunner = new ETLRunner();
		Logger logger = LoggerFactory.getLogger(ETLRunner.class);
		//can use by other monitor program
		String tips = "etl run result=";
		
		if (args.length < 1) {
			etlRunner.printUsage();
			logger.info(tips + "-1");
			System.exit(-1);
		}

		ShellContext context = ShellContext.getInstance();
		context.parseArgs(args);

		String configFile = context.getValue("configFile", null);
		String processId = context.getValue("processId", null);

		// init <contextProperty location="/redis.properties" />
		// must init before new AbstractProcess class
		ContextProperty ctxProperty = ContextProperty.getInstance();
		ctxProperty.init(configFile);

		Element processNode = etlRunner.getProcessNode(configFile, processId);
		if (processNode == null) {
			logger.error("get null process node.");
			logger.info(tips + "-1");
			System.exit(-1);
		}
		
		LookupMgr lookupMgr = LookupMgr.getInstance();
		lookupMgr.init(configFile);
		LKPTableMgr lkpTableMgr = lookupMgr.getLkpTableMgr();
		
		WriteLog writeLog = WriteLog.getInstance();
		writeLog.init(configFile);

		AbstractProcess process = AbstractProcess.newInstance((Element) processNode);
		try {
			// init process
			process.init((Element) processNode);
			// add context to process
			process.setContext(context);
		} catch (ParseException e) {
			e.printStackTrace();
			
			logger.info(tips + "-1");
			System.exit(-1);
		}
		
		IRunner runner = null;
		if (process.getType().equals(Constants.DEFAULT_COMPUTING_FRAMEWORK_MR)) {
			//map/reduce computing framework
			Configuration conf = new Configuration();

			//only write log record state date to database
			conf.set(Constants.LOG_RECORD_STAT_DATE,
					context.getValue(Constants.CONTEXT_MONTH, ""));

			runner = new MRRunner(conf, (MRProcess) process, lkpTableMgr,writeLog);	
		}else if(process.getType().equals(LocalFileProcess.class.getSimpleName())){
			//local file computing framework
			runner = new LocalFileRunner((LocalFileProcess)process);
		}else if(process.getType().equals(Constants.PROCESS_SLICE_LOCAL_FILE)){
			runner = new SliceLocalFileRunner((LocalFileProcess)process);
		}

		try {
			int ret = runner.etlRun();
			logger.info(tips + ret);
		} catch (ETLException e) {
			e.printStackTrace();
			logger.info(tips + "-1");
		}
	}
}

package com.gsta.bigdata.etl.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.process.MRProcess;
import com.gsta.bigdata.utils.BeansUtils;

public class GZCZReducer extends Reducer<Text, Text, Text, Text> {
	private final static String STOP_STATUS = "stopStatus";
	private final static String DELIMITER = "gzczDelimiter";
	private String statusField;
	private String delimiter;
	private MRProcess process;
	private Text outText = new Text();  
	private Logger logger = LoggerFactory.getLogger(getClass());

	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		
		String json = context.getConfiguration().get(
				Constants.HADOOP_CONF_MRPROCESS);
		this.process = (MRProcess) BeansUtils.json2obj(json, MRProcess.class);
		if (this.process == null) {
			throw new InterruptedException("process is null.");
		}
		
		this.statusField = this.process.getConf(STOP_STATUS);
		if(this.statusField == null){
			throw new InterruptedException("gzcz stop status was't be set in computingFrameworkConfigs.");
		}
		this.delimiter = this.process.getConf(DELIMITER, "\\$");
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context) throws IOException,
			InterruptedException {
		if(key == null){
			logger.error("reducer key is null.");
			return;
		}
		if(values == null){
			logger.error("account=" + key.toString() + ",values is null.");
			return;
		}
		
		Iterator<Text> iter = values.iterator();
		String ret = null;
		while(iter.hasNext()){
			String str = iter.next().toString();
			if(str.length() <= 0){
				continue;
			}
			
			String[] data = str.split(this.delimiter);
			//only save status is not stop status
			if(data.length > 0 && !data[16].equals(this.statusField)){
				ret = str;
				break;
			}
		}
		
		if(ret != null){
			this.outText.set(ret);
			context.write(null, this.outText);
		}
	}

	@Override
	protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
	}
}

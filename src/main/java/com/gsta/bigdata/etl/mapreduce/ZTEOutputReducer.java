package com.gsta.bigdata.etl.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ETLProcess;
import com.gsta.bigdata.utils.BeansUtils;

public class ZTEOutputReducer extends Reducer<Text, Text, Text, Text> {
	private ETLProcess process;
	private String filePrefix;
	private int reduceTaskCount;
	private int outputFileCount;
	private int totalFileCount = 1;
	private MultipleOutputs<Text, Text> multiOutput ; 
	private Text outValue = new Text();  
	
	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		
		String json = context.getConfiguration().get(
				Constants.HADOOP_CONF_ETLPROCESS);
		this.process = (ETLProcess) BeansUtils.json2obj(json, ETLProcess.class);
		if (this.process == null) {
			throw new InterruptedException("process is null.");
		}
		
		//this.taskId = context.getTaskAttemptID().getTaskID().getId();
		
		this.filePrefix = this.process.getOutputFilePrefix();
		this.reduceTaskCount = Integer.parseInt(this.process.getConf(
				Constants.HADOOP_REDUCE_TASKS, "1"));
		this.outputFileCount = Integer.parseInt(this.process.getConf(
				Constants.CF_REDUCE_OUTPUT_FILE_COUNT, "1"));
		this.totalFileCount = this.outputFileCount * this.reduceTaskCount;
		
		this.multiOutput = new MultipleOutputs<Text, Text>(context);
	}
	
	private String getOutputFileName(int enodeid) {
		int idx = enodeid % this.totalFileCount;
		return this.filePrefix + idx ;
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		if (key == null || values == null) {
			return;
		}

		int enodeid = Integer.parseInt(key.toString());
		Iterator<Text> iter = values.iterator();
		while (iter.hasNext()) {
			this.outValue.set(iter.next().toString());
			this.multiOutput.write(this.getOutputFileName(enodeid), null,
					this.outValue);
		}
	}

	@Override
	protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
		this.multiOutput.close();
	}
	
	public final static void setMultipleOutputs(Job job, ETLProcess process) {
		if (job == null || process == null) {
			return;
		}
		
		String filePrefix = process.getOutputFilePrefix();
		int reduceTaskCount = Integer.parseInt(process.getConf(
				Constants.HADOOP_REDUCE_TASKS, "0"));
		int outputFileCount = Integer.parseInt(process.getConf(
				Constants.CF_REDUCE_OUTPUT_FILE_COUNT, "0"));
		
		for (int i = 0; i < reduceTaskCount; i++) {
			for (int j = 0; j < outputFileCount; j++) {
				int idx = i * outputFileCount + j;
				String namedOutput = filePrefix + idx ;
				MultipleOutputs.addNamedOutput(job, namedOutput,
						TextOutputFormat.class, Text.class, Text.class);
			}
		}
	}

	public static void  main(String[] args){
		int taskCount = 10;
		int fileCount = 50;
		for(int i=0;i<taskCount;i++)
			for(int j=0;j<fileCount;j++){
				int idx = i * fileCount + j;
				String namedOutput = "mr_data_" + idx + "." + "txt";
				System.out.println(namedOutput);
			}
		int x = 853906 /50 % 10;
		System.out.println("idx=" + x);
	}
}

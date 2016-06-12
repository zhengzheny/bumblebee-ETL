package com.gsta.bigdata.etl.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class ETLHashPartition extends HashPartitioner<Text, Text> {
	@Override
	public int getPartition(Text key, Text value, int numReduceTasks) {
		int intKey = 0;
		try {
			intKey = Integer.parseInt(key.toString());
		} catch (NumberFormatException e) {
			intKey = 0;
		}

		return intKey % numReduceTasks;
	}
}

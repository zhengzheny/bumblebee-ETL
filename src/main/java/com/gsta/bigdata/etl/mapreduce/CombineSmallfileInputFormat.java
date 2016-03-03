package com.gsta.bigdata.etl.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

public class CombineSmallfileInputFormat extends
		CombineFileInputFormat<Object, Text> {
	@Override
	public RecordReader<Object, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		CombineFileSplit combineFileSplit = (CombineFileSplit) split;

		CombineFileRecordReader<Object, Text> recordReader = new CombineFileRecordReader<Object, Text>(
				combineFileSplit, context, CombineSmallfileRecordReader.class);

		try {
			recordReader.initialize(combineFileSplit, context);
		} catch (InterruptedException e) {
			new RuntimeException(
					"Error to initialize CombineSmallfileRecordReader.");
		}

		return recordReader;
	}
}

package com.gsta.bigdata.etl;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import kafka.serializer.StringDecoder;

import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.ETLProcess;
import com.gsta.bigdata.etl.core.source.KafkaStream;
import com.gsta.bigdata.etl.core.source.ValidatorException;
import com.gsta.bigdata.etl.hdfs.ValueOutputFormat;

public class SparkStreamingRunner implements IRunner,Serializable {
	private static final long serialVersionUID = 1L;
	private Logger logger = LoggerFactory.getLogger(getClass());
	private ETLProcess process;
	
	public SparkStreamingRunner(ETLProcess process) {
		this.process = process;
	}
	
	@SuppressWarnings("serial")
	@Override
	public int etlRun() throws ETLException {
		String brokers = null, topics = null;
		if (process.getSourceMetaData() instanceof KafkaStream) {
			KafkaStream kafkaStream = (KafkaStream) process.getSourceMetaData();
			brokers = kafkaStream.getBrokers();
			topics = kafkaStream.getTopics();
		} 
		
		if (brokers == null || "".equals(brokers)) {
			throw new ETLException("kafka brokers is null.");
		}
		if (topics == null || "".equals(topics)) {
			throw new ETLException("kafka topics is null.");
		}
		
		String processId = this.process.getId();
		//default duration is 10 second.
		long duration = Long.parseLong(process.getConf(
				Constants.CF_SPARK_DURATION, "10"));
		
		logger.info("processId=" + processId );
		logger.info("brokers=" + brokers + ",topics=" + topics);
		logger.info("kafka duration is " + duration + " second.");
		
		SparkConf sparkConf = new SparkConf().setAppName(processId);
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,
				Durations.seconds(duration));

		HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
		HashMap<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", brokers);
	    
	    JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
	            jssc,
	            String.class,
	            String.class,
	            StringDecoder.class,
	            StringDecoder.class,
	            kafkaParams,
	            topicsSet
	        );
	    
		JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
			@Override
	        public String call(Tuple2<String, String> tuple2) {
	        	return tuple2._2();
	        }
	      });
		
		JavaDStream<String> dpis = lines.map(new Function<String,String>(){
			public String call(String dpi){
				return parseLine(dpi);
			}
		});
		
		JavaPairDStream<String, String> pairDPIs = dpis.mapToPair(new PairFunction<String, String, String>() {
			@Override
          public Tuple2<String, String> call(String s) {
				if(s == null || "".endsWith(s)){
					return null;
				}
				return new Tuple2<String, String>(s, "");
          }
        });
		
		String path = process.getOutputPath();
		String suffix = "stream";
		
		pairDPIs.saveAsNewAPIHadoopFiles(path, suffix, Text.class, Text.class, ValueOutputFormat.class);
		//pairDPIs.saveAsNewAPIHadoopFiles(path, suffix, Text.class, Text.class, TextOutputFormat.class);				
		
		//only for debug
		/*pairDPIs.foreachRDD(new Function<JavaPairRDD<String,String>, Void>(){
	    	@Override
	          public Void call(JavaPairRDD<String,String> rdds)
	              throws Exception {
	    		rdds.collect().stream().forEach(System.out::println);
	    		logger.info(" hello spark streaming...");
	    		return null;
	    	}
	    });*/
		
	    jssc.start();
	    jssc.awaitTermination();
		return 0;
	}

	private String parseLine(String line){
		if(line == null || "".equals(line)){
			return null;
		}
		
		try {
			//deal right DPI data,the invalid or error DPI,ignore and do nothing
			ETLData data = this.process.parseLine(line, null);
			if (data != null) {
				this.process.onTransform(data);
				return this.process.getOutputValue(data);
			}
		} catch (ETLException | ValidatorException e) {
			logger.debug(e.toString());
		}
		
		return null;
	}
}

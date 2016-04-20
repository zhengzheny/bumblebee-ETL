package com.gsta.bigdata.etl;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.ETLProcess;
import com.gsta.bigdata.etl.core.TransformException;
import com.gsta.bigdata.etl.core.source.KafkaStream;
import com.gsta.bigdata.etl.core.source.ValidatorException;
import com.gsta.bigdata.etl.mapreduce.OnlyKeyOutputFormat;

@Deprecated
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
		String topics = null,zkroot = null, group = null;
		//default duration is 10 second.
		long duration = 10;
		int receivesNum = 1;
		int partitionsNum = 1;
		
		if (process.getSourceMetaData() instanceof KafkaStream) {
			KafkaStream kafkaStream = (KafkaStream) process.getSourceMetaData();
			
			topics = kafkaStream.getTopics();
			zkroot = kafkaStream.getZkroot();
			group = kafkaStream.getGroup();
			receivesNum = kafkaStream.getReceivesNum();
			duration = kafkaStream.getDuration();
			partitionsNum = kafkaStream.getPartitionsNum();
		} 
		
		if (zkroot == null || "".equals(zkroot)) {
			throw new ETLException("kafka zkroot is null.");
		}
		if (topics == null || "".equals(topics)) {
			throw new ETLException("kafka topics is null.");
		}
		if(group == null){
			group = "etl-consumer";
		}
		
		String processId = this.process.getId();
				
		logger.info("processId=" + processId );
		logger.info("kafka info:");
		logger.info("zkroot=" + zkroot );
		logger.info("topics=" + topics );
		logger.info("duration=" + duration + " second.");
		logger.info("topic thread number=" + receivesNum );
		logger.info("group=" + group );
		
		SparkConf sparkConf = new SparkConf().setAppName(processId);
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,
				Durations.seconds(duration));
		
		/*HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
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
	        );*/
	    Map<String, Integer> topicMap = new HashMap<String, Integer>();
	    String[] arrTopics = topics.split(",");
	    for (String topic : arrTopics) {
	      topicMap.put(topic, receivesNum);
	    }
	    
	    JavaPairReceiverInputDStream<String, String> messages =
	            //KafkaUtils.createStream(jssc, zkroot, group, topicMap);
	    		KafkaUtils.createStream(jssc, zkroot, group, topicMap,StorageLevel.MEMORY_AND_DISK_SER_2());	
	   
		JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
			@Override
	        public String call(Tuple2<String, String> tuple2) {
				//logger.info("tuple=" + tuple2._2());
	        	return tuple2._2();
	        }
	      });
	    
		
		JavaDStream<String> dpis = lines.map(new Function<String,String>(){
			public String call(String dpi){
				//logger.info("dpi=" + dpi);
				return dpi;
				//return parseLine(dpi);
			}
		});
		
		JavaPairDStream<String, String> pairDPIs = dpis.mapToPair(new PairFunction<String, String, String>() {
			@Override
          public Tuple2<String, String> call(String s) {
				//logger.info("s=" + s);
				return new Tuple2<String, String>(s, "");
          }
        });
		
		JavaPairDStream<String, String> splitPairDpis = pairDPIs.repartition(partitionsNum);
		
		String path = process.getOutputPath();
		String suffix = "";
		
		//pairDPIs.persist(StorageLevel.MEMORY_AND_DISK_SER());
		try{
			splitPairDpis.saveAsNewAPIHadoopFiles(path, suffix, Text.class, Text.class, OnlyKeyOutputFormat.class);
		}catch(Exception e){
			logger.error(e.toString());
		}
		
		/*JavaDStream<Long> dpiCounts = splitPairDpis.count();
		
		dpiCounts.foreachRDD(new Function<JavaRDD<Long>, Void>(){
	    	@Override
	          public Void call(JavaRDD<Long> rdds) throws Exception {
	    		List<Long> counters = rdds.collect();
	    		for(Long c:counters){
	    			logger.info("dpi count=" + c);
	    		}
	    		
	    		return null;
	    	}
	    });*/
		
	    jssc.start();
	    jssc.awaitTermination();
		return 0;
	}

	@SuppressWarnings("unused")
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
		}catch(TransformException e){
			logger.debug(e.toString());
		}
		
		return null;
	}
}

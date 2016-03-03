package com.gsta.bigdata.etl;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;

import kafka.producer.KeyedMessage;

import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.cloudera.spark.streaming.kafka.JavaDStreamKafkaWriter;
import org.cloudera.spark.streaming.kafka.JavaDStreamKafkaWriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.ETLProcess;
import com.gsta.bigdata.etl.core.source.KafkaStream;
import com.gsta.bigdata.etl.core.source.ValidatorException;
import com.gsta.bigdata.etl.mapreduce.OnlyKeyOutputFormat;

import consumer.kafka.MessageAndMetadata;
import consumer.kafka.ReceiverLauncher;

public class SparkKafkaRunner implements IRunner ,Serializable{
	private static final long serialVersionUID = 1L;
	private Logger logger = LoggerFactory.getLogger(getClass());
	private ETLProcess process;
	
	private final static String RESULT_MODE_HDFS = "hdfs";
	private final static String RESULT_MODE_KAFKA = "kafka";
	
	public SparkKafkaRunner(ETLProcess process){
		this.process = process;
	}
	
	private Properties getReceiveKafkaConf(KafkaStream kafkaStream){
		Properties props = new Properties();
		props.put("zookeeper.hosts", kafkaStream.getHosts());
		props.put("zookeeper.port",kafkaStream.getPort());
		props.put("zookeeper.broker.path", kafkaStream.getBrokers());
		props.put("kafka.topic", kafkaStream.getTopics());
		props.put("kafka.consumer.id", kafkaStream.getGroup());
		props.put("zookeeper.consumer.connection", kafkaStream.getConsumerZK());
		props.put("zookeeper.consumer.path", kafkaStream.getConsumerZKPath());
		// Optional Properties
		props.put("consumer.forcefromstart", kafkaStream.getForcefromstart());
		props.put("consumer.fetchsizebytes", kafkaStream.getFetchsizebytes());
		props.put("consumer.fillfreqms", "250");
		props.put("consumer.backpressure.enabled", kafkaStream.getBackpressure());
		props.put("kafka.message.handler.class",
				"consumer.kafka.IdentityMessageHandler");
		
		return props;
	}
	
	private void printInfo(KafkaStream kafkaStream){
		logger.info("\nprocessId=" + process.getId() + 
				"\nconfig:" + kafkaStream.toString() +
				"\noutput:\n" +
				"outputPath=" + this.process.getOutputPath() + 
				"\nkafka brokers=" + this.process.getOutputKafkaBrokers() +
				"\nkafka topic=" + this.process.getOutputKafkaTopic());
	}
	
	@SuppressWarnings("serial")
	@Override
	public int etlRun() throws ETLException {
		if(this.process == null){
			throw new ETLException("process object is null...");
		}
		KafkaStream kafkaStream = null;
		if (process.getSourceMetaData() instanceof KafkaStream) {
			kafkaStream = (KafkaStream) process.getSourceMetaData();
		}
		if(kafkaStream == null){
			throw new ETLException("kafkaStream object is null,maybe config is wrong...");
		}
		this.printInfo(kafkaStream);
		
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName(process.getId());
		JavaStreamingContext jsc = new JavaStreamingContext(sparkConf,
				Durations.seconds(kafkaStream.getDuration()));
		
		// Specify number of Receivers you need.
		int receiversNum = kafkaStream.getReceivesNum();
		JavaDStream<MessageAndMetadata> unionStreams = ReceiverLauncher.launch(
				jsc, this.getReceiveKafkaConf(kafkaStream), receiversNum,
				kafkaStream.getStorageLevel());

		JavaDStream<String> dpis = unionStreams
				.map(new Function<MessageAndMetadata, String>() {
					@Override
					public String call(MessageAndMetadata tuple) {
						String payload = new String(tuple.getPayload());
						return parseLine(payload);
					}
				}).filter(new Function<String, Boolean>() {
					public Boolean call(String dpi) {
						if (dpi == null) {
							return false;
						}
						return true;
					}
				});
		
		String resultMode = kafkaStream.getResultMode();
		if(RESULT_MODE_HDFS.equals(resultMode)){
			this.writeHdfs(dpis, this.process.getOutputPath(), kafkaStream);
		}else if(RESULT_MODE_KAFKA.equals(resultMode)){
			this.writeKafka(dpis);
		}
			
		JavaDStream<Long> dpiCounts = dpis.count();
		dpiCounts.foreachRDD(new Function<JavaRDD<Long>, Void>() {
			@Override
			public Void call(JavaRDD<Long> rdds) throws Exception {
				List<Long> counters = rdds.collect();
				for (Long c : counters) {
					logger.info("dpi count=" + c);
				}

				return null;
			}
		});
		
		jsc.start();
		jsc.awaitTermination();

		return 0;
	}
	
	private void writeKafka(JavaDStream<String> dpis) {
		Properties producerConf = new Properties();
		producerConf.put("serializer.class", "kafka.serializer.DefaultEncoder");
		producerConf.put("key.serializer.class","kafka.serializer.StringEncoder");
		producerConf.put("metadata.broker.list",this.process.getOutputKafkaBrokers());
		producerConf.put("request.required.acks", "1");

		String topic = this.process.getOutputKafkaTopic();
		JavaDStreamKafkaWriter<String> writer = JavaDStreamKafkaWriterFactory
				.fromJavaDStream(dpis);

		writer.writeToKafka(producerConf, new ProcessingFunc(topic));
	}
	
	@SuppressWarnings("serial")
	class ProcessingFunc implements
			Function<String, KeyedMessage<String, byte[]>> {
		private String topic = "default-topic";
		public  ProcessingFunc(String topic){
			this.topic = topic;
		}

		public KeyedMessage<String, byte[]> call(String in) throws Exception {
			if(in == null){
				return new KeyedMessage<String, byte[]>(this.topic, null,null);
			}		
			return new KeyedMessage<String, byte[]>(this.topic, null,in.getBytes());
		}
	}
	
	@SuppressWarnings("serial")
	private void writeHdfs(JavaDStream<String> dpis, String path,
			KafkaStream kafkaStream) {
		JavaPairDStream<String, String> pairDPIs = dpis
				.mapToPair(new PairFunction<String, String, String>() {
					@Override
					public Tuple2<String, String> call(String s) {
						return new Tuple2<String, String>(s, "");
					}
				});
		JavaPairDStream<String, String> splitPairDpis = pairDPIs
				.repartition(kafkaStream.getPartitionsNum());

		String suffix = "";
		try {
			splitPairDpis.saveAsNewAPIHadoopFiles(path, suffix, Text.class,
					Text.class, OnlyKeyOutputFormat.class);
		} catch (Exception e) {
			logger.error(e.toString());
		}
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
			logger.error(e.toString());
		}
		
		return null;
	}
}

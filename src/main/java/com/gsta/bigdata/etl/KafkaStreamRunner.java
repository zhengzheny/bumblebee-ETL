package com.gsta.bigdata.etl;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.ETLProcess;
import com.gsta.bigdata.etl.core.TransformException;
import com.gsta.bigdata.etl.core.source.KafkaStream;
import com.gsta.bigdata.etl.core.source.ValidatorException;

import java.io.Serializable;
import java.util.Properties;

public class KafkaStreamRunner implements IRunner ,Serializable{
	private static final long serialVersionUID = 6596737900295124205L;
	private ETLProcess process;
	private Logger logger = LoggerFactory.getLogger(getClass());

	public KafkaStreamRunner(ETLProcess process) {
		super();
		this.process = process;
	}

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
		logger.info(kafkaStream.toString());
		
		Properties props = new Properties();
		//kafka stream config
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, kafkaStream.getApp_id());
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaStream.getBrokers());
		props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, kafkaStream.getZookeeper());
		props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.CLIENT_ID_CONFIG, kafkaStream.getClient_id());		
		props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, kafkaStream.getTimestamp_extractor());
		props.put(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, kafkaStream.getBuffered_records_per_partition());
		props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, kafkaStream.getNum_stream_threads());
		props.put(StreamsConfig.POLL_MS_CONFIG, kafkaStream.getPoll_ms());
		props.put(StreamsConfig.STATE_DIR_CONFIG, kafkaStream.getState_dir());
		props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, kafkaStream.getCache_max_bytes_buffering());
		//producer config
		props.put(ProducerConfig.ACKS_CONFIG,kafkaStream.getAcks());
		props.put(ProducerConfig.BATCH_SIZE_CONFIG,kafkaStream.getBatch_size());
		props.put(ProducerConfig.LINGER_MS_CONFIG,kafkaStream.getLinger_ms());
		props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,kafkaStream.getMax_request_size());
		//consumer config
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaStream.getAuto_offset_reset());
		props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, kafkaStream.getMax_partition_fetch_bytes());
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, kafkaStream.getMax_poll_records());
		
		
		KStreamBuilder builder = new KStreamBuilder();
		KStream<String, String> source = builder.stream(kafkaStream.getInputTopic());
		source.map(new KeyValueMapper<String,String,KeyValue<String, String>>(){
			@Override
			public KeyValue<String, String> apply(String key, String value) {
				//System.out.println("key="+key+",value="+value);
				String v = parseLine(value);
				return new KeyValue<>(key, v);
			}
		}).filter(new Predicate<String, String>(){
			@Override
			public boolean test(String key, String value) {
				if(value == null){
					return false;
				}
				
				return true;
			}
		}).to(kafkaStream.getOutputTopic());

		final KafkaStreams streams = new KafkaStreams(builder, props);
		streams.start();
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				try {
					logger.info("The JVM Hook is execute...");
					streams.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
		
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
		} catch (ETLException | ValidatorException|TransformException e) {
			logger.warn(e.toString());
		}
		
		return null;
	}
}

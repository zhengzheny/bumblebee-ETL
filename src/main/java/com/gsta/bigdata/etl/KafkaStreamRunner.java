package com.gsta.bigdata.etl;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;

import com.gsta.bigdata.etl.core.ETLProcess;

import java.io.Serializable;
import java.util.Properties;

public class KafkaStreamRunner implements IRunner ,Serializable{
	private static final long serialVersionUID = 6596737900295124205L;
	private ETLProcess process;

	public KafkaStreamRunner(ETLProcess process) {
		super();
		this.process = process;
	}

	public static void main(String[] args) throws Exception {
		if(args.length < 5){
			
		}
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka stream test");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "10.17.35.109:9069");
		props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "10.17.35.109:2181/kafkaFlumeTest");
		props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String()
				.getClass().getName());
		props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String()
				.getClass().getName());
		props.put(StreamsConfig.CLIENT_ID_CONFIG, "gdnoce");

		// setting offset reset to earliest so that we can re-run the demo code
		// with the same pre-loaded data
		//props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		KStreamBuilder builder = new KStreamBuilder();

		KStream<String, String> source = builder.stream("flumetest");

		/*KTable<String, Long> counts = source
				.flatMapValues(new ValueMapper<String, Iterable<String>>() {
					@Override
					public Iterable<String> apply(String value) {
						return Arrays.asList(value.toLowerCase(
								Locale.getDefault()).split(" "));
					}
				})
				.map(new KeyValueMapper<String, String, KeyValue<String, String>>() {
					@Override
					public KeyValue<String, String> apply(String key,
							String value) {
						return new KeyValue<>(value, value);
					}
				}).groupByKey().count("Counts");

		// need to override value serde to Long type
		counts.to(Serdes.String(), Serdes.Long(), "streams-wordcount-output");*/
		source.map(new KeyValueMapper<String,String,KeyValue<String, String>>(){
			@Override
			public KeyValue<String, String> apply(String key, String value) {
				System.out.println("key=" + key + ",value=" + value);
				return new KeyValue<>(key, value);
			}
		}).filter(new Predicate<String, String>(){
			@Override
			public boolean test(String key, String value) {
				if(value == null){
					return false;
				}
				
				return true;
			}
			
		}).to("streams-wordcount-output");

		final KafkaStreams streams = new KafkaStreams(builder, props);
		
		
		streams.start();
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				try {
					System.out.println("The JVM Hook is execute");
					streams.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});

		// usually the stream application would be running forever,
		// in this example we just let it run for some time and stop since the
		// input data is finite.
		//Thread.sleep(5000000L);
		//streams.close();
	}

	@Override
	public int etlRun() throws ETLException {
		// TODO Auto-generated method stub
		return 0;
	}

}

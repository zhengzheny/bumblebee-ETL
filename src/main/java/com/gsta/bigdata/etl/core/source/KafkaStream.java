package com.gsta.bigdata.etl.core.source;

import javax.xml.xpath.XPathExpressionException;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.gsta.bigdata.etl.core.ChildrenTag;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.utils.XmlTools;

public class KafkaStream extends SimpleFlat {
	private static final long serialVersionUID = -7777739195563572315L;
	private String app_id;
	private String brokers;
	private String zookeeper;
	private String inputTopic;
	private String outputTopic;
	private String client_id;
	private String auto_offset_reset;
	private String timestamp_extractor;
	private String buffered_records_per_partition;
	private String num_stream_threads;
	private String poll_ms;
	private String state_dir;
	private String cache_max_bytes_buffering;
	private String acks;
	private String batch_size;
	private String linger_ms;
	private String max_request_size;
	private String max_partition_fetch_bytes;
	private String max_poll_records;
	
	private final static String APP_ID = "app_id";
	private final static String BROKERS = "brokers";
	private final static String ZOOKEEPER = "zookeeper";
	private final static String INPUT_TOPIC = "inputTopic";
	private final static String OUTPUT_TOPIC = "outputTopic";
	private final static String CLIENT_ID = "client_id";
	private final static String AUTO_OFFSET_RESET="auto_offset_reset";
	private final static String TIMESTAMP_EXTRACTOR= "timestamp_extractor";
	private final static String BUFFERED_RECORDS_PER_PARTITION = "buffered_records_per_partition";
	private final static String NUM_STREAM_THREADS = "num.stream.threads";
	private final static String POLL_MS = "poll.ms";
	private final static String STATE_DIR = "state.dir";
	private final static String CACHE_MAX_BYTES_BUFFERING = "cache.max.bytes.buffering";
	private final static String ACKS = "acks";
	private final static String BATCH_SIZE = "batch.size";
	private final static String LINGER_MS = "linger.ms";
	private final static String MAX_REQUEST_SIZE = "max.request.size";
	private final static String MAX_PARTITION_FETCH_BYTES = "max.partition.fetch.bytes";
	private final static String MAX_POLL_RECORDS = "max.poll.records";
	
	public KafkaStream() {
		super();
		
		super.registerChildrenTags(new ChildrenTag(
				Constants.PATH_SOURCE_METADATA_KAFKA, ChildrenTag.NODE));
	}

	@Override
	protected void createChildNode(Element node) throws ParseException {
		Preconditions.checkNotNull(node, "element is null");
		
		super.createChildNode(node);
		
		if (node.getNodeType() == Node.ELEMENT_NODE) {
			NodeList nodeList = node.getChildNodes();
			
			for (int i = 0; i < nodeList.getLength(); i++) {
				Node subNode = nodeList.item(i);
				if (subNode.getNodeType() == Node.ELEMENT_NODE){
					String name = subNode.getNodeName();
					String value = null;
					try {
						value = XmlTools.getNodeValue((Element)subNode);					
						switch(name){
						case APP_ID:
							this.app_id = value;
							break;
						case BROKERS:
							this.brokers = value;
							break;
						case ZOOKEEPER:
							this.zookeeper = value;
							break;
						case INPUT_TOPIC:
							this.inputTopic = value;
							break;
						case OUTPUT_TOPIC:
							this.outputTopic = value;
							break;
						case CLIENT_ID:
							this.client_id = value;
							break;
						case AUTO_OFFSET_RESET:
							this.auto_offset_reset = value;
							break;
						case TIMESTAMP_EXTRACTOR:
							this.timestamp_extractor = value;
							break;
						case BUFFERED_RECORDS_PER_PARTITION:
							this.buffered_records_per_partition = value;
							break;
						case NUM_STREAM_THREADS:
							this.num_stream_threads = value;
							break;
						case POLL_MS:
							this.poll_ms = value;
							break;
						case STATE_DIR:
							this.state_dir = value;
							break;
						case CACHE_MAX_BYTES_BUFFERING:
							this.cache_max_bytes_buffering = value;
							break;
						case ACKS:
							this.acks = value;
							break;
						case BATCH_SIZE:
							this.batch_size = value;
							break;
						case LINGER_MS:
							this.linger_ms = value;
							break;
						case MAX_REQUEST_SIZE:
							this.max_request_size = value;
							break;
						case MAX_PARTITION_FETCH_BYTES:
							this.max_partition_fetch_bytes = value;
							break;
						case MAX_POLL_RECORDS:
							this.max_poll_records = value;
							break;
						}
					} catch (XPathExpressionException e) {
						e.printStackTrace();
					}
				}
			}//end for
		}//end if
	}
	
	public String getBuffered_records_per_partition() {
		return buffered_records_per_partition;
	}

	public String getAcks() {
		return acks;
	}

	public String getMax_poll_records() {
		return max_poll_records;
	}

	public String getBatch_size() {
		return batch_size;
	}

	public String getMax_partition_fetch_bytes() {
		return max_partition_fetch_bytes;
	}

	public String getMax_request_size() {
		return max_request_size;
	}

	public String getLinger_ms() {
		return linger_ms;
	}

	public String getCache_max_bytes_buffering() {
		return cache_max_bytes_buffering;
	}

	public String getState_dir() {
		return state_dir;
	}

	public String getNum_stream_threads() {
		return num_stream_threads;
	}

	public String getPoll_ms() {
		return poll_ms;
	}

	public String getApp_id() {
		return app_id;
	}

	public String getBrokers() {
		return brokers;
	}

	public String getZookeeper() {
		return zookeeper;
	}

	public String getInputTopic() {
		return inputTopic;
	}

	public String getOutputTopic() {
		return outputTopic;
	}

	public String getClient_id() {
		return client_id;
	}

	public String getAuto_offset_reset() {
		return auto_offset_reset;
	}

	public String getTimestamp_extractor() {
		return timestamp_extractor;
	}
}

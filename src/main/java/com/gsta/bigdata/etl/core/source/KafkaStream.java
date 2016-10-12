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
	
	private final static String APP_ID = "app_id";
	private final static String BROKERS = "brokers";
	private final static String ZOOKEEPER = "zookeeper";
	private final static String INPUT_TOPIC = "inputTopic";
	private final static String OUTPUT_TOPIC = "outputTopic";
	private final static String CLIENT_ID = "client_id";
	private final static String AUTO_OFFSET_RESET="auto_offset_reset";
	private final static String TIMESTAMP_EXTRACTOR= "timestamp_extractor";

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
						}
					} catch (XPathExpressionException e) {
						e.printStackTrace();
					}
				}
			}//end for
		}//end if
	}

	public String toString(){
		return "kafka stream config:\napp_id=" + this.app_id +
				"\nbrokers=" + this.brokers +
				"\nclient_id=" + this.client_id +
				"\nzookeeper=" + this.zookeeper +
				"\ninputTopic=" + this.inputTopic +
				"\noutputTopic=" + this.outputTopic;
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

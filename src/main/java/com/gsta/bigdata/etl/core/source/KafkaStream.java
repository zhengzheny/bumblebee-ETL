package com.gsta.bigdata.etl.core.source;

import org.w3c.dom.Element;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.gsta.bigdata.etl.core.ChildrenTag;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ContextMgr;
import com.gsta.bigdata.etl.core.ParseException;

public class KafkaStream extends SimpleFlat {
	private static final long serialVersionUID = -3695999187190213893L;
	@JsonProperty
	private String brokers ;
	@JsonProperty
	private String topics;
	
	public KafkaStream() {
		super();
		
		super.registerChildrenTags(new ChildrenTag(
				Constants.PATH_SOURCE_METADATA_KAFKA, ChildrenTag.NODE));
	}
	
	@Override
	protected void createChildNode(Element node) throws ParseException {
		Preconditions.checkNotNull(node, "element is null");

		super.createChildNode(node);

		if (node.getNodeName().equals(Constants.PATH_SOURCE_METADATA_KAFKA)) {
			this.brokers = ContextMgr.getValue(
					node.getAttribute(Constants.ATTR_BROKERS));
			this.topics = ContextMgr.getValue(
					node.getAttribute(Constants.ATTR_TOPICS));
		}
	}

	public String getBrokers() {
		return brokers;
	}

	public String getTopics() {
		return topics;
	}
}

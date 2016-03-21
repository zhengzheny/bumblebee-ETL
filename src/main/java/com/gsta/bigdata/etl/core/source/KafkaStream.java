package com.gsta.bigdata.etl.core.source;

import org.apache.spark.storage.StorageLevel;
import org.w3c.dom.Element;

import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.gsta.bigdata.etl.core.ChildrenTag;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ContextMgr;
import com.gsta.bigdata.etl.core.ParseException;

public class KafkaStream extends SimpleFlat {
	private static final long serialVersionUID = -3695999187190213893L;
	
	private String hosts ;	
	private String port = "2181";	
	private String consumerZK ;	
	private String consumerZKPath ;	
	private String fetchsizebytes = "1048576";	
	private String backpressure = "true";	
	private String brokers ;	
	private String topics;
	@Deprecated	
	private String zkroot;	
	private String group;	
	private int receivesNum = 1;	
	private long duration;	
	private int partitionsNum = 1;	
	private String forcefromstart = "true";	
	private int storageLevel = 0;	
	private String resultMode = "hdfs";
	private String fillfreqms;
	private boolean checkPoint = false;
	private String chkPointPath;
	
	public KafkaStream() {
		super();
		
		super.registerChildrenTags(new ChildrenTag(
				Constants.PATH_SOURCE_METADATA_KAFKA, ChildrenTag.NODE));
	}
	
	@SuppressWarnings("deprecation")
	@Override
	protected void createChildNode(Element node) throws ParseException {
		Preconditions.checkNotNull(node, "element is null");

		super.createChildNode(node);

		if (node.getNodeName().equals(Constants.PATH_SOURCE_METADATA_KAFKA)) {
			//TODO::throw null exception
			this.hosts = ContextMgr.getValue(
					node.getAttribute(Constants.ATTR_HOSTS));
			this.port = ContextMgr.getValue(
					node.getAttribute(Constants.ATTR_PORT));
			
			this.consumerZK = ContextMgr.getValue(
					node.getAttribute(Constants.ATTR_CONSUMER_ZK));
			this.consumerZKPath = ContextMgr.getValue(
					node.getAttribute(Constants.ATTR_CONSUMER_ZK_PATH));
			this.fetchsizebytes = ContextMgr.getValue(
					node.getAttribute(Constants.ATTR_FETCHSIZE_BYTES));
			
			this.backpressure = ContextMgr.getValue(
					node.getAttribute(Constants.ATTR_BACK_PRESSURE));
			this.fillfreqms = ContextMgr.getValue(
					node.getAttribute(Constants.ATTR_FILL_FREQMS));
			this.forcefromstart = ContextMgr.getValue(
					node.getAttribute(Constants.ATTR_FORCE_FROM_START));
			this.resultMode = ContextMgr.getValue(
					node.getAttribute(Constants.ATTR_RESULT_MODE));
			
			this.brokers = ContextMgr.getValue(
					node.getAttribute(Constants.ATTR_BROKERS));
			this.topics = ContextMgr.getValue(
					node.getAttribute(Constants.ATTR_TOPIC));
			this.zkroot = ContextMgr.getValue(
					node.getAttribute(Constants.ATTR_ZK_ROOT));
			this.group = ContextMgr.getValue(
					node.getAttribute(Constants.ATTR_GROUP));
			
			String temp = ContextMgr.getValue(
					node.getAttribute(Constants.ATTR_RECEIVES_NUM));
			if(temp !=null && !"".equals(temp)){
				this.receivesNum = Integer.parseInt(temp);
			}
			
			temp = ContextMgr.getValue(
					node.getAttribute(Constants.ATTR_DURATION));
			if(temp != null && !"".equals(temp)){
				this.duration = Long.parseLong(temp);
			}
			
			temp = ContextMgr.getValue(
					node.getAttribute(Constants.ATTR_PARTITIONS_NUM));
			if(temp != null && !"".equals(temp)){
				this.partitionsNum = Integer.parseInt(temp);
			}
			
			temp = ContextMgr.getValue(
					node.getAttribute(Constants.ATTR_STORAGE_LEVEL));
			if(temp != null && !"".equals(temp)){
				this.storageLevel = Integer.parseInt(temp);
			}
			
			temp = ContextMgr.getValue(
					node.getAttribute(Constants.ATTR_CHK_POINT));
			if(temp != null && "true".equals(temp)){
				this.checkPoint = true;
			}
			
			this.chkPointPath = ContextMgr.getValue(
					node.getAttribute(Constants.ATTR_CHK_POINT_PATH));
		}
	}

	public boolean isCheckPoint() {
		return checkPoint;
	}

	public String getChkPointPath() {
		return chkPointPath;
	}

	public String getResultMode() {
		return resultMode;
	}
	
	public String getBrokers() {
		return brokers;
	}

	public String getTopics() {
		return topics;
	}

	public String getZkroot() {
		return zkroot;
	}

	public String getGroup() {
		return group;
	}

	public int getReceivesNum() {
		return receivesNum;
	}

	public int getPartitionsNum() {
		return partitionsNum;
	}

	public String getHosts() {
		return hosts;
	}

	public String getPort() {
		return port;
	}

	public String getConsumerZK() {
		return consumerZK;
	}

	public String getConsumerZKPath() {
		return consumerZKPath;
	}

	public String getFetchsizebytes() {
		return fetchsizebytes;
	}

	public String getBackpressure() {
		return backpressure;
	}

	public long getDuration() {
		return duration;
	}
	
	public String getForcefromstart() {
		return forcefromstart;
	}
	
	public StorageLevel getStorageLevel() {
		switch (this.storageLevel) {
		case 0:
			return StorageLevel.MEMORY_ONLY();
		case 1:
			return StorageLevel.MEMORY_ONLY_2();
		case 2:
			return StorageLevel.MEMORY_ONLY_SER();
		case 3:
			return StorageLevel.MEMORY_ONLY_SER_2();
		case 4:
			return StorageLevel.MEMORY_AND_DISK();
		case 5:
			return StorageLevel.MEMORY_AND_DISK_2();
		case 6:
			return StorageLevel.MEMORY_AND_DISK_SER();
		case 7:
			return StorageLevel.MEMORY_AND_DISK_SER_2();
		}
		
		return StorageLevel.MEMORY_ONLY();
	}

	public String getFillfreqms() {
		return fillfreqms;
	}

	public String toString(){
		StringBuffer sb  = new StringBuffer();
		
		sb.append("hosts=").append(this.hosts)
				.append("\nport=").append(this.port)
				.append("\nbrokers=").append(this.brokers)
				.append("\ntopics=").append(this.topics)
				.append("\ngroup=").append(this.group)
				.append("\nconsumerZK=").append(this.consumerZK)
				.append("\nconsumerZKPath=").append(this.consumerZKPath)
				.append("\nbackpressure=").append(this.backpressure)
				.append("\nfillfreqms=").append(this.fillfreqms)
				.append("\nfetchsizebytes=").append(this.fetchsizebytes)
				.append("\nforcefromstart=").append(this.forcefromstart)
				.append("\nstorageLevel=").append(this.getStorageLevel())				
				.append("\nduration=").append(this.duration)
				.append("\nreceivesNum=").append(this.receivesNum)
				.append("\nresultMode=").append(this.resultMode)
				.append("\npartitionsNum=").append(this.partitionsNum)
				.append("\ncheckPoint=").append(this.checkPoint)
				.append("\ncheckPointPath=").append(this.chkPointPath);
		
		return sb.toString();
	}
}

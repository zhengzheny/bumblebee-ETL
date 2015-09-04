package com.gsta.bigdata.etl.core.function;

import java.util.Iterator;
import java.util.Map;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.ChildrenTag;
import com.gsta.bigdata.etl.core.ShellContext;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.etl.core.function.model.RedisKey;
import com.gsta.bigdata.etl.core.function.model.RedisValue;

/**
 * abstract redis function
 * <function name="" output="">
 * 	<key fields="f1,f2" delimiter="+"/>
 * 	<value fields="f3,f4 delimiter="+"/>
 * </function>
 * @author tianxq
 *
 */
public abstract class AbstractRedisFunc extends AbstractFunction {
	public final static String ATTR_EXPIRE_TIME = "expireTime";
	public final static String PATH_KEY = "key";
	public final static String PATH_VALUE = "value";
	public final static String REDIS_ARGU_COUNT_DELI = "*";
	public final static String REDIS_COMMAND_LEN_DELI = "$";
	public final static String REDIS_LINE_BREAK = "\r\n";
	private final static String REDIS_EXPIRE_CMD = "expire";
	public final static String CONTEXT_MONTH = "month";
	
	@JsonProperty
	private RedisKey redisKey;
	@JsonProperty
	private RedisValue redisValue;
	//data expire time in redis
	@JsonProperty
	private int expireTime = -1;

	public AbstractRedisFunc() {
		super();
		
		// have key and value element
		super.registerChildrenTags(new ChildrenTag(
				AbstractRedisFunc.PATH_KEY,ChildrenTag.NODE));
		super.registerChildrenTags(new ChildrenTag(
				AbstractRedisFunc.PATH_VALUE, ChildrenTag.NODE));
	}

	@Override
	public abstract String onCalculate(Map<String, String> functionData,
			ShellContext context) throws ETLException;
	
	@Override
	protected void initAttrs(Element element) throws ParseException {
		super.initAttrs(element);
		
		String str = super.getAttr(AbstractRedisFunc.ATTR_EXPIRE_TIME);
		if(str != null && !"".equals(str)){
			//the unit is day in configure file
			this.expireTime = Integer.parseInt(str) * 24 * 3600;
		}
	}

	@Override
	protected void createChildNode(Element node) throws ParseException {
		Preconditions.checkNotNull(node, "element is null");
		
		super.createChildNode(node);
		
		if(node.getNodeName().equals(AbstractRedisFunc.PATH_KEY)){
			this.redisKey = new RedisKey();
			this.redisKey.init(node);
		}
		
		if(node.getNodeName().equals(AbstractRedisFunc.PATH_VALUE)){
			this.redisValue = new RedisValue();
			this.redisValue.init(node);
		}
	}

	@Override
	protected void createChildNodeList(NodeList nodeList) throws ParseException {
		super.createChildNodeList(nodeList);
		//has no child node list
	}
	
	/**
	 * get redis key 
	 * @param functionData
	 * @return
	 */
	@JsonIgnore
	protected String getKey(Map<String, String> functionData,int month){
		StringBuffer sb = new StringBuffer();
		String delimiter = this.redisKey.getDelimiter();

		Iterator<String> iter = this.redisKey.getFields().iterator();
		while (iter.hasNext()) {
			String value = functionData.get(iter.next());
			sb.append(value).append(delimiter);
		}

		String strRedisKey = sb.toString();
		//if month=-1,key is the defined field,or the last part of key is month mod 3.
		//this is only meet CC's demand,but do not affect others to use.
		//month'value from context which comes from shell command.
		if (month == -1) {
			strRedisKey = strRedisKey.substring(0,strRedisKey.length() - delimiter.length());
		} else {
			// only 3 month in redis
			int i = month % this.redisKey.getMonthMode();
			strRedisKey = strRedisKey + i;
		}

		return strRedisKey;
	}
	
	/**
	 * get redis expire command
	 * *3          //expire command,key,expire time
	 * $6          //expire command length
	 * expire      //expire command
	 * $11         //key length
	 * 13312345678 //key value
	 * $2          //expire time length
	 * 60          //expire time:60 second
	 * @return
	 */
	protected String getExpireCMD(String key){
		Preconditions.checkNotNull(key, "redis key is null.");

		StringBuffer ret = new StringBuffer();
		// argument count = expire command + key + expire time
		int arguCount = 3;
		String strArguCount = AbstractRedisFunc.REDIS_ARGU_COUNT_DELI
				+ arguCount + AbstractRedisFunc.REDIS_LINE_BREAK;
		// argument count
		ret.append(strArguCount);

		// expire command length
		String strExpireCMDLen = AbstractRedisFunc.REDIS_COMMAND_LEN_DELI
				+ AbstractRedisFunc.REDIS_EXPIRE_CMD.length()
				+ AbstractRedisFunc.REDIS_LINE_BREAK;
		String strExpireCMD = AbstractRedisFunc.REDIS_EXPIRE_CMD
				+ AbstractRedisFunc.REDIS_LINE_BREAK;
		// expire command
		ret.append(strExpireCMDLen).append(strExpireCMD);

		// key length
		String redisKeyLen = AbstractRedisFunc.REDIS_COMMAND_LEN_DELI + key.length()
				+ AbstractRedisFunc.REDIS_LINE_BREAK;
		//key
		String redisKey = key+ AbstractRedisFunc.REDIS_LINE_BREAK;
		ret.append(redisKeyLen).append(redisKey);

		// value
		String tmpExpireTime = String.valueOf(this.expireTime);
		String expireTimeLen = AbstractRedisFunc.REDIS_COMMAND_LEN_DELI
				+ tmpExpireTime.length() + AbstractRedisFunc.REDIS_LINE_BREAK;
		String strExpireTime = this.expireTime
				+ AbstractRedisFunc.REDIS_LINE_BREAK;
		ret.append(expireTimeLen).append(strExpireTime);

		// write line break
		ret.append(AbstractRedisFunc.REDIS_LINE_BREAK);

		return ret.toString();
	}

	public RedisKey getRedisKey() {
		return redisKey;
	}

	public RedisValue getRedisValue() {
		return redisValue;
	}

	public int getExpireTime() {
		return expireTime;
	}
}

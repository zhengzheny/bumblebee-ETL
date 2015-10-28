package com.gsta.bigdata.etl.core.function;

import java.util.Iterator;
import java.util.Map;

import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.ShellContext;

/**
 * redis set object:key,value
 * 
 * flat file data,for example
 * field:key|CONTACT_NBR
 * data:13312345678|30
 * 
 * redis set protocol,for example
 * *3             //four argument:set,key,value
 * $3        	  //redis command "set" length
 * set           //set command
 * $11            //key length
 * 13312345678    //key's value
 * $2             //value length
 * 30            //value
  * redis setex protocol,for example
 * *4             //four argument:setex,key,expireTime,value
 * $5        	  //redis command "setex" length
 * setex           //set command
 * $11            //key length
 * 13312345678    //key's value
 * $2             //expire time
 * 60             //expire time:60 second
 * $2             //value length
 * 30            //value
 * every field end with \r\n
 * every line end wih \r\n
 * @author tianxq
 *
 */
public class RedisSet extends AbstractRedisFunc {
	private static final long serialVersionUID = -4536580943269431588L;
	private final static String COL_SET = "set";
	private final static String COL_SETEX = "setex";
	
	public RedisSet(){
		super();
	}
	
	@Override
	protected String onCalculate(Map<String, String> functionData,
			ShellContext context) throws ETLException {
		//default value is -1
		String strMonth = context.getValue(AbstractRedisFunc.CONTEXT_MONTH, "-1");
		int month = Integer.parseInt(strMonth);
		
		String key = super.getKey(functionData,month);
		String value = this.getValue(functionData);

		// has no expire time
		if (super.getExpireTime() < 0) {
			return this.getSetProtocol(key, value);
		} else {
			return this.getSetEXProtocol(key, value);
		}
	}
	
	/**
	 * get set value
	 * @param functionData
	 * @return
	 */
	private String getValue(Map<String, String> functionData){
		StringBuffer sb = new StringBuffer();
		String delimiter = super.getRedisValue().getDelimiter();

		Iterator<String> iter = super.getRedisValue().getFields().iterator();
		while(iter.hasNext()){
			String value = functionData.get(iter.next());
			sb.append(value).append(delimiter);
		}
		
		String redisValue = sb.toString();
		if(redisValue.endsWith(delimiter)){
			redisValue = redisValue.substring(0,redisValue.length() - delimiter.length());
		}
				
		return redisValue;
	}

	private String getSetProtocol(String key,String value){
		StringBuffer sb = new StringBuffer();
		
		//argument count = set command + key  + value
		int arguCount = 3;
		String strArguCount = AbstractRedisFunc.REDIS_ARGU_COUNT_DELI + 
				arguCount + AbstractRedisFunc.REDIS_LINE_BREAK;
		//argument count
		sb.append(strArguCount);
		
		//set command length
		String colSetLen =  AbstractRedisFunc.REDIS_COMMAND_LEN_DELI + 
				RedisSet.COL_SET.length() + AbstractRedisFunc.REDIS_LINE_BREAK;
		//set command
		String colSet = RedisSet.COL_SET + AbstractRedisFunc.REDIS_LINE_BREAK;
		sb.append(colSetLen).append(colSet);
		
		//key length
		String redisKeyLen = AbstractRedisFunc.REDIS_COMMAND_LEN_DELI + 
				key.length() + AbstractRedisFunc.REDIS_LINE_BREAK;
		//key
		String redisKey = key + AbstractRedisFunc.REDIS_LINE_BREAK;
		sb.append(redisKeyLen).append(redisKey);
		
		//value length
		String redisValueLen = AbstractRedisFunc.REDIS_COMMAND_LEN_DELI + 
				value.length() + AbstractRedisFunc.REDIS_LINE_BREAK;
		String strRedisValue = value + AbstractRedisFunc.REDIS_LINE_BREAK;
		sb.append(redisValueLen).append(strRedisValue);

		//write line break
		sb.append(AbstractRedisFunc.REDIS_LINE_BREAK);
		
		return sb.toString();
	}
	
	/**
	 * get set expire redis protocol
	 * @return
	 */
	private String getSetEXProtocol(String key,String value){
		StringBuffer sb = new StringBuffer();
		
		//argument count = set command + key + expire time + value
		int arguCount = 4;
		String strArguCount = AbstractRedisFunc.REDIS_ARGU_COUNT_DELI + 
				arguCount + AbstractRedisFunc.REDIS_LINE_BREAK;
		//argument count
		sb.append(strArguCount);
		
		//set command
		String colSetEXLen =  AbstractRedisFunc.REDIS_COMMAND_LEN_DELI + 
				RedisSet.COL_SETEX.length() + AbstractRedisFunc.REDIS_LINE_BREAK;
		String colSetEX = RedisSet.COL_SETEX + AbstractRedisFunc.REDIS_LINE_BREAK;
		//set command
		sb.append(colSetEXLen).append(colSetEX);
		
		//key
		String redisKeyLen = AbstractRedisFunc.REDIS_COMMAND_LEN_DELI + 
				key.length() + AbstractRedisFunc.REDIS_LINE_BREAK;
		String redisKey = key + AbstractRedisFunc.REDIS_LINE_BREAK;
		sb.append(redisKeyLen).append(redisKey);
		
		//expire time
		String tmpExpireTime = String.valueOf(super.getExpireTime());
		String expireTimeLen = AbstractRedisFunc.REDIS_COMMAND_LEN_DELI + 
				tmpExpireTime.length() + AbstractRedisFunc.REDIS_LINE_BREAK;
		String strExpireTime = super.getExpireTime() + AbstractRedisFunc.REDIS_LINE_BREAK;
		sb.append(expireTimeLen).append(strExpireTime);
		
		//value length
		String valueLen = AbstractRedisFunc.REDIS_COMMAND_LEN_DELI + 
				value.length() + AbstractRedisFunc.REDIS_LINE_BREAK;
		//value
		String strRedisValue = value + AbstractRedisFunc.REDIS_LINE_BREAK;
		sb.append(valueLen).append(strRedisValue);
		
		//write line break
		sb.append(AbstractRedisFunc.REDIS_LINE_BREAK);
		
		return sb.toString();	
	}
	
	@Override
	protected Map<String, String> multiOutputOnCalculate(
			Map<String, String> functionData, ShellContext context)
			throws ETLException {
		return null;
	}
}

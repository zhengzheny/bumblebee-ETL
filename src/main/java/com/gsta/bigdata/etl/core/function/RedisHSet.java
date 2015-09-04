package com.gsta.bigdata.etl.core.function;

import java.util.Iterator;
import java.util.Map;

import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.ShellContext;

/**
 * hdfs transform redis protocl file
 * redis hmset object:key,map<key,value>
 * 
 * flat file data,for example
 * 
 * redis hmset protocol,for example
 * *4             //four argument:hmset,key,field1
 * $5        	  //redis command "hmset" length
 * hmset           //hmset command
 * $11            //key length
 * 18979177429    //key's value
 * $3             //field name "sex" length
 * sex            //field name
 * $4             //field value length
 * male           //field value 
 * every field end with \r\n
 * every line end wih \r\n
 * 
 * 
data source:
CDMA_NBR|i_shenfenzhen|i_zaiwangshijian
13302200190|0|(54,++)
*4
$5
hmset
$11
13302200190
$13
i_shenfenzhen
$0


*4
$5
hmset
$11
13302200190
$16
i_zaiwangshijian
$7
(54,++)

 * @author tianxq
 *
 */
public class RedisHSet extends AbstractRedisFunc {
	private final static String COL_HSET = "hmset";
	
	public RedisHSet(){
		super();
	}
	
	@Override
	public String onCalculate(Map<String, String> functionData,
			ShellContext context) throws ETLException {
		StringBuffer sb = new StringBuffer();
		
		//default value is -1
		String strMonth = context.getValue(AbstractRedisFunc.CONTEXT_MONTH, "-1");
		int month = Integer.parseInt(strMonth);
		
		String key = super.getKey(functionData,month);
		
		sb.append(this.getHMSetProtocol(key,functionData));
		
		// get expire command
		if (super.getExpireTime() > 0) {
			sb.append(super.getExpireCMD(key));
		}

		return sb.toString();
	}

	/**
	 * get redis hmset protocol
	 * 
	 * @return
	 */
	private String getHMSetProtocol(String key,Map<String, String> functionData) {
		StringBuffer sb = new StringBuffer();

		// argument count = hmset command + key + field name + field value
		int arguCount = 4;
		String strArguCount = AbstractRedisFunc.REDIS_ARGU_COUNT_DELI
				+ arguCount + AbstractRedisFunc.REDIS_LINE_BREAK;

		// hmset command
		String colHsetLen = AbstractRedisFunc.REDIS_COMMAND_LEN_DELI
				+ RedisHSet.COL_HSET.length()
				+ AbstractRedisFunc.REDIS_LINE_BREAK;
		String colHset = RedisHSet.COL_HSET
				+ AbstractRedisFunc.REDIS_LINE_BREAK;

		// key
		String redisKeyLen = AbstractRedisFunc.REDIS_COMMAND_LEN_DELI
				+ key.length() + AbstractRedisFunc.REDIS_LINE_BREAK;
		String redisKey = key + AbstractRedisFunc.REDIS_LINE_BREAK;

		// field
		Iterator<String> iter = super.getRedisValue().getFields().iterator();
		while (iter.hasNext()) {
			String field = iter.next();
			String value = functionData.get(field);

			// argument count
			sb.append(strArguCount);

			// hmset command
			sb.append(colHsetLen).append(colHset);

			// key
			sb.append(redisKeyLen).append(redisKey);

			// field name
			String fieldLen = AbstractRedisFunc.REDIS_COMMAND_LEN_DELI
					+ field.length() + AbstractRedisFunc.REDIS_LINE_BREAK;
			String fieldName = field + AbstractRedisFunc.REDIS_LINE_BREAK;
			sb.append(fieldLen).append(fieldName);

			// field value
			String redisValueLen = AbstractRedisFunc.REDIS_COMMAND_LEN_DELI
					+ value.length() + AbstractRedisFunc.REDIS_LINE_BREAK;
			String redisValue = value + AbstractRedisFunc.REDIS_LINE_BREAK;
			sb.append(redisValueLen).append(redisValue);

			// write line break
			sb.append(AbstractRedisFunc.REDIS_LINE_BREAK);
		}

		return sb.toString();
	}
}

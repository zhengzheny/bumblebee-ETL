package com.gsta.bigdata.etl.core.function.dpi;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * @author xiangy
 *
 */
public class UseragentCacheCleaner implements Runnable {
	public static final int CACHE_CLEAN_MIN_INTERVAL = 1000;  
	public static final float CACHE_CLEAN_MIN_RATIO = 0.7f;   
	private Map<String, MatchedUseragent> cachedMap;
	private int cleanInterval;
	private int cacheSize;
	private float cleanRatio;
	private boolean stopped = false;
	
	
	/**
	 * @param ruleCacheMap
	 * @param cleanInterval
	 */
	public UseragentCacheCleaner(Map<String, MatchedUseragent> cachedMap, int cacheSize, int cleanInterval, float cleanRatio)
	{
		this.cachedMap = cachedMap;
		this.cacheSize = cacheSize;
		this.cleanInterval = cleanInterval * 1000;
		if (this.cleanInterval < CACHE_CLEAN_MIN_INTERVAL)
		{
			this.cleanInterval = CACHE_CLEAN_MIN_INTERVAL;
		}
		this.cleanRatio = cleanRatio;
		if (this.cleanRatio < 0 || this.cleanRatio > 1)
		{
			this.cleanRatio = CACHE_CLEAN_MIN_RATIO;
		}
		this.stopped = false;
	}
	
	/**
	 */
	public void kill()
	{
		this.stopped = true;
	}
	
	/**
	 */
	public void run()
	{
		if (cachedMap == null || cacheSize <= 0)
		{
			System.out.println("The CacheMap is null. Please check it. ");
			return;
		}

		
		try
		{
			while (!stopped)
			{
				Thread.sleep(cleanInterval);
				cleanCache();
			}
		} catch (Exception e)
		{
		}
	}
	
	/**
	 */
	private void cleanCache()
	{
		if (this.cachedMap == null || cachedMap.isEmpty() || cacheSize == 0)
		{
			return;
		}
		
		if ((cachedMap.size() + 0.0) / cacheSize < this.cleanRatio)
		{
			return;
		}
		
		Set<String> keySet = cachedMap.keySet(); 
		Iterator<String> cacheIterator = keySet.iterator(); 
		long oldMapSize = cachedMap.size();
		long currTime = System.currentTimeMillis();
		while(cacheIterator.hasNext()){ 
			String useragent = cacheIterator.next();
			if (useragent == null)
			{
				continue;
			}
			MatchedUseragent mua = cachedMap.get(useragent);
			if (mua == null)
			{
				continue;
			}
			long diffTime = currTime - mua.getUpdateTime();
			if (diffTime > this.cleanInterval)
			{
				cachedMap.remove(useragent);
			}
			
			if ((cachedMap.size() + 0.0) / this.cacheSize < this.cleanRatio)
			{
				break;
			}
		}
		System.out.printf("debug---------------Useragent cache is cleaned. the cache size from % to %. \r\n", oldMapSize, cachedMap.size());
	}
}

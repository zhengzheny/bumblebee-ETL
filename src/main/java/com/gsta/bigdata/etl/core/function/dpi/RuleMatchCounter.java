package com.gsta.bigdata.etl.core.function.dpi;

import java.util.concurrent.atomic.AtomicLong;

public class RuleMatchCounter {
	public AtomicLong totalCount = new AtomicLong();
	
	public RuleMatchCounter() {
		totalCount.getAndSet(0);
	}
	
	public void reset()
	{
		totalCount.getAndSet(0);
	}
	
}

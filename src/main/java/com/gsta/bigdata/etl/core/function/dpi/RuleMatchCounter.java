package com.gsta.bigdata.etl.core.function.dpi;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

public class RuleMatchCounter implements Serializable{
	private static final long serialVersionUID = -689731327368124817L;
	
	public AtomicLong totalCount = new AtomicLong();
	
	public RuleMatchCounter() {
		totalCount.getAndSet(0);
	}
	
	public void reset()
	{
		totalCount.getAndSet(0);
	}
	
}

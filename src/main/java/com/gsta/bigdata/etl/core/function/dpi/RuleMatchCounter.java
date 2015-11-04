package com.gsta.bigdata.etl.core.function.dpi;

public class RuleMatchCounter {
	public long totalCount;
	
	public RuleMatchCounter() {
		totalCount = 0;
	}
	
	public void reset()
	{
		totalCount = 0;
	}
}

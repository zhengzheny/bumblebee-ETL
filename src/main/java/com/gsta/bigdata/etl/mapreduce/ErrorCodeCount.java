package com.gsta.bigdata.etl.mapreduce;

import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ErrorCodeCount {
	@JsonProperty
	private String errorCode;
	//message is the sampling of error 
	@JsonProperty
	private String errorMessage;
	@JsonProperty
	private AtomicLong count = new AtomicLong(1);

	public ErrorCodeCount() {
	}

	public ErrorCodeCount(String errorCode, String errorMessage) {
		this.errorCode = errorCode;
		this.errorMessage = errorMessage;
	}

	public void addCount(){
		this.count.addAndGet(1);
	}
	
	public void addCount(long count){
		this.count.addAndGet(count);
	}

	public String getErrorCode() {
		return this.errorCode;
	}
	
	public long getCount(){
		return this.count.longValue();
	}

	public String toString(){
		String info = "errorCode=" + this.errorCode + 
				",errroCount=" + this.count +
				",message=" + this.errorMessage ;
		
		return info;
	}
}

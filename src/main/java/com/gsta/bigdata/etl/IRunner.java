package com.gsta.bigdata.etl;

/**
 * etl runner interface
 * 
 * @author tianxq
 * 
 */
public interface IRunner {
	public int etlRun() throws ETLException;
	
}

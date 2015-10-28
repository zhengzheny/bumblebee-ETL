package com.gsta.bigdata.etl.core;

import java.io.Serializable;

/**
 * 
 * @author tianxq
 *
 */
public class LoadException extends RuntimeException implements Serializable{
	private static final long serialVersionUID = 1L;

	public LoadException() {
	}

	public LoadException(String message) {
		super(message);
	}

	public LoadException(Throwable cause) {
		super(cause);
	}

	public LoadException(String message, Throwable cause) {
		super(message, cause);
	}

	public LoadException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}

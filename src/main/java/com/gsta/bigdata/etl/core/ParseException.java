package com.gsta.bigdata.etl.core;

import java.io.Serializable;

/**
 * 
 * @author tianxq
 *
 */
public class ParseException extends RuntimeException implements Serializable{
	private static final long serialVersionUID = 6310171400625681940L;

	public ParseException() {
	}

	public ParseException(String message) {
		super(message);
	}

	public ParseException(Throwable cause) {
		super(cause);
	}

	public ParseException(String message, Throwable cause) {
		super(message, cause);
	}

	public ParseException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}

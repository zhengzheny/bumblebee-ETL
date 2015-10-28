package com.gsta.bigdata.etl;

import java.io.Serializable;

/**
 * 
 * @author Shine
 *
 */
public abstract class AbstractException extends RuntimeException implements Serializable{
	private static final long serialVersionUID = -7503545262174556387L;
	
	protected String errorCode;

	public AbstractException() {
	}

	public AbstractException(String message) {
		super(message);
	}
	
	public AbstractException(String errorCode,String message) {
		super(message);
		this.errorCode = errorCode;
	}

	public AbstractException(Throwable cause) {
		super(cause);
	}

	public AbstractException(String message, Throwable cause) {
		super(message, cause);
	}

	public AbstractException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public String getErrorCode() {
		return errorCode;
	}
}

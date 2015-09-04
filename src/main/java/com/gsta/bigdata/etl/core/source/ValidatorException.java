package com.gsta.bigdata.etl.core.source;

import com.gsta.bigdata.etl.AbstractException;

/**
 * 
 * @author tianxq
 *
 */
public class ValidatorException extends AbstractException {
	private static final long serialVersionUID = 1L;
	
	public final static String VALIDATOR = "20";
	
	public final static String NULL_FIELD_NAMES = VALIDATOR + "001";
	public final static String TYPE_NOT_MATCH = VALIDATOR + "002";
	public final static String FIELD_MIN_LENGTH = VALIDATOR + "003";
	public final static String FIELD_MAX_LENGTH = VALIDATOR + "004";
	public final static String OTHER_EXCEPTION = VALIDATOR + "005";
	
	public ValidatorException() {
		super();
	}

	public ValidatorException(String message) {
		super(message);
	}
	
	public ValidatorException(Throwable cause) {
		super(cause);
	}
	
	public ValidatorException(String errorCode,String message) {
		super(errorCode,message);
	}
	
	public ValidatorException(String message, Throwable cause) {
		super(message, cause);
	}

	public ValidatorException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}

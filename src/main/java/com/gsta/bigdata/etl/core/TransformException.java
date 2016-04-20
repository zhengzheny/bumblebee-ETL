package com.gsta.bigdata.etl.core;

import java.io.Serializable;

import com.gsta.bigdata.etl.AbstractException;

/**
 * 
 * @author tianxq
 *
 */
public class TransformException extends AbstractException implements Serializable{
	private static final long serialVersionUID = 6310171400625681940L;

	public TransformException() {
	}

	public TransformException(String message) {
		super(message);
	}

	public TransformException(Throwable cause) {
		super(cause);
	}

	public TransformException(String message, Throwable cause) {
		super(message, cause);
	}

	public TransformException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}

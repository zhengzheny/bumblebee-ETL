package com.gsta.bigdata.etl;

import java.io.Serializable;

/**
 * 
 * @author tianxq
 * 
 */
public class ETLException extends AbstractException implements Serializable{
	private static final long serialVersionUID = -5580249479791459587L;
	
	public final static String MAPREDUCE = "10";
	public final static String CORE = "11";
	public final static String PROCESS = "12";
	public final static String SOURCE = "13";
	public final static String FILTER = "14";
	public final static String FUNCTION = "15";
	public final static String LOOKUP = "16";
	public final static String OUTPUT = "17";

	public final static String NULL_SOURCE_META = SOURCE + "001";
	public final static String NULL_DATA_BY_SPLIT = SOURCE + "002";
	public final static String DATA_NOT_EQUAL_DEFINITION = SOURCE + "003";
	public final static String NULL_LINE_TRIM = SOURCE + "004";
	public final static String UNSUPPORTED_ENCODING = SOURCE + "005";
	public final static String DATA_LENGTH_NOT_EQUAL_LAST_POS = SOURCE + "006";

	public final static String NULL_FIELD_NAMES = OUTPUT + "001";

	public final static String FILTER_ACCEPT = FILTER + "001";

	public ETLException() {
		super();
	}

	public ETLException(String message) {
		super(message);
	}

	public ETLException(String errorCode, String message) {
		super(errorCode, message);
	}

	public ETLException(Throwable cause) {
		super(cause);
	}

	public ETLException(String message, Throwable cause) {
		super(message, cause);
	}

	public ETLException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}

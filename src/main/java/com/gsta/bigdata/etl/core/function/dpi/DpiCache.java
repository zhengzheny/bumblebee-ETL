package com.gsta.bigdata.etl.core.function.dpi;


/**
 * parse function cache
 * 
 * @author shine
 *
 */
public class DpiCache {
	private String size;
	private String cleanInterval;
	private String cleanRatio;

	public final static String ATTR_SIZE = "size";
	public final static String ATTR_CLEANINTERVAL = "cleanInterval";
	public final static String ATTR_CLEANRATIO = "cleanRatio";

	public DpiCache() {

	}

	public void setSize(String size) {
		this.size = size;
	}

	public void setCleanInterval(String cleanInterval) {
		this.cleanInterval = cleanInterval;
	}

	public void setCleanRatio(String cleanRatio) {
		this.cleanRatio = cleanRatio;
	}

	public String getSize() {
		return size;
	}

	public String getCleanInterval() {
		return cleanInterval;
	}

	public String getCleanRatio() {
		return cleanRatio;
	}

}
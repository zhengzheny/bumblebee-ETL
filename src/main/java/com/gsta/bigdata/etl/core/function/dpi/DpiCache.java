package com.gsta.bigdata.etl.core.function.dpi;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.core.AbstractETLObject;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ParseException;

/**
 * parse function cache
 * 
 * @author shine
 *
 */
public class DpiCache extends AbstractETLObject {
	private static final long serialVersionUID = 3730164067546516461L;
	
	@JsonProperty
	private String size;
	@JsonProperty
	private String cleanInterval;
	@JsonProperty
	private String cleanRatio;

	public final static String ATTR_SIZE = "size";
	public final static String ATTR_CLEANINTERVAL = "cleanInterval";
	public final static String ATTR_CLEANRATIO = "cleanRatio";

	public DpiCache() {
		super();

		super.tagName = Constants.PATH_DPI_CACHE;
	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		this.size = super.getAttr(ATTR_SIZE);
		this.cleanInterval = super.getAttr(ATTR_CLEANINTERVAL);
		this.cleanRatio = super.getAttr(ATTR_CLEANRATIO);
	}

	@Override
	protected void createChildNode(Element node) throws ParseException {
		// has no child
	}

	@Override
	protected void createChildNodeList(NodeList nodeList) throws ParseException {
		// has no child list
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
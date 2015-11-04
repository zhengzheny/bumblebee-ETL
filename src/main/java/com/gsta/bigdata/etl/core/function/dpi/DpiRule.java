package com.gsta.bigdata.etl.core.function.dpi;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.core.AbstractETLObject;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ParseException;

/**
 *  parse function rule
 *  
 *  @author shine
 */
public class DpiRule extends AbstractETLObject {
	private static final long serialVersionUID = 3387417710216086627L;
	
	@JsonProperty
	private String id;
	@JsonProperty
	private String filePath;
	@JsonProperty
	private String statisFileDir;
	
	public final static String ATTR_ID = "id";
	public final static String ATTR_FILEPATH = "filePath";
	public final static String ATTR_STATISFILEDIR = "statisFileDir";

	public DpiRule() {
		super();
		
		super.tagName = Constants.PATH_DPI_RULE;
	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		this.id = super.getAttr(ATTR_ID);
		this.filePath = super.getAttr(ATTR_FILEPATH);
		this.statisFileDir = super.getAttr(ATTR_STATISFILEDIR);
	}

	@Override
	protected void createChildNode(Element node) throws ParseException {
		//has no child 
	}

	@Override
	protected void createChildNodeList(NodeList nodeList)
			throws ParseException {
		//has no child list
	}

	public String getId() {
		return id;
	}

	public String getFilePath() {
		return filePath;
	}

	public String getStatisFileDir() {
		return statisFileDir;
	}
	
}
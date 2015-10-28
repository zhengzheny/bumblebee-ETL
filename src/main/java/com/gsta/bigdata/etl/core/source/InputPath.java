package com.gsta.bigdata.etl.core.source;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.gsta.bigdata.etl.core.AbstractETLObject;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ParseException;
/**
 * local file input path
 * 
 * @author tianxq
 *
 */
public class InputPath extends AbstractETLObject {
	private static final long serialVersionUID = -7771220326045083253L;
	@JsonProperty
	private String path;
	@JsonProperty
	private String fileSuffix = "xml";
	@JsonProperty
	private String fileNamePattern ;
	@JsonProperty
	private String charset = "utf-8";
	
	public InputPath() {
		super.tagName = Constants.TAG_INPUTPATH;
	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		Preconditions.checkNotNull(element, "element is null");

		this.path = super.getAttr(Constants.ATTR_PATH);
		this.fileSuffix = super.getAttr(Constants.ATTR_FILE_SUFFIX,"xml");
		this.fileNamePattern = super.getAttr(Constants.ATTR_FILENAME_PATTERN);		
		this.charset = super.getAttr(Constants.ATTR_CHARSET,"utf-8");
	}

	@Override
	protected void createChildNode(Element node) throws ParseException {
		//has no child node
	}

	@Override
	protected void createChildNodeList(NodeList nodeList) throws ParseException {
		//has no child node list
	}

	public String getPath() {
		return path;
	}

	public String getFileSuffix() {
		return fileSuffix;
	}

	public String getFileNamePattern() {
		return fileNamePattern;
	}

	public String getCharset() {
		return charset;
	}
}

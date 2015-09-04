package com.gsta.bigdata.etl.core.process;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.ChildrenTag;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.OutputMetaData;
import com.gsta.bigdata.etl.core.ParseException;
/**
 * local file process
 * 
 * @author tianxq
 *
 */
public class LocalFileProcess extends AbstractProcess {
	@JsonProperty
	private OutputMetaData outputMetaData;

	public LocalFileProcess() {
		super();

		super.registerChildrenTags(new ChildrenTag(
				Constants.PATH_OUTPUT_METADATA, ChildrenTag.NODE));
	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		super.initAttrs(element);
	}

	@Override
	protected void createChildNode(Element node) throws ParseException {
		Preconditions.checkNotNull(node, "element  is null");

		super.createChildNode(node);

		if (node.getNodeName().equals(Constants.PATH_OUTPUT_METADATA)) {
			this.outputMetaData = new OutputMetaData();
			this.outputMetaData.init(node);
		}
	}

	@Override
	protected void createChildNodeList(NodeList nodeList) throws ParseException {
		super.createChildNodeList(nodeList);
	}

	@Override
	public String getOutputValue(ETLData data) throws ETLException{
		if (this.outputMetaData == null) {
			return null;
		}

		return this.outputMetaData.getOutputValue(data);
	}

	@Override
	public String getOutputPath() {
		if (this.outputMetaData != null) {
			return this.outputMetaData.getOutputPath();
		}

		return null;
	}

	@Override
	public String getErrorPath() {
		if (this.outputMetaData != null) {
			return this.outputMetaData.getErrorPath();
		}

		return null;
	}

	public OutputMetaData getOutputMetaData() {
		return this.outputMetaData;
	}
}

package com.gsta.bigdata.etl.core.process;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.ChildrenTag;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.etl.mapreduce.MROutputMetaData;

/**
 * hadoop map/reduce process
 * 
 * @author tianxq
 * 
 */
public class MRProcess extends AbstractProcess {
	@JsonProperty
	private MROutputMetaData mrOutputMetaData;

	public MRProcess() {
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
			this.mrOutputMetaData = new MROutputMetaData();
			this.mrOutputMetaData.init(node);
		}
	}

	@Override
	protected void createChildNodeList(NodeList nodeList) throws ParseException {
		super.createChildNodeList(nodeList);
	}

	@Override
	public String getOutputValue(ETLData data) throws ETLException {
		if (this.mrOutputMetaData == null) {
			return null;
		}

		return this.mrOutputMetaData.getOutputValue(data);
	}

	/**
	 * get etl output key value,only for map/reduce computing framework
	 * 
	 * @param data
	 * @return
	 */
	public String getOutputKey(ETLData data) throws ETLException {
		if (this.mrOutputMetaData == null) {
			return null;
		}

		return this.mrOutputMetaData.getOutputKey(data);
	}

	@Override
	public String getOutputPath() {
		if (this.mrOutputMetaData != null) {
			return this.mrOutputMetaData.getOutputPath();
		}

		return null;
	}

	@Override
	public String getErrorPath() {
		if (this.mrOutputMetaData != null) {
			return this.mrOutputMetaData.getErrorPath();
		}

		return null;
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(super.toString());
		sb.append("\r\nmapOutputMetaData:\r\n");

		if (this.mrOutputMetaData != null) {
			sb.append(this.mrOutputMetaData.toString());
		}

		return sb.toString();
	}

	public MROutputMetaData getMrOutputMetaData() {
		return mrOutputMetaData;
	}
}

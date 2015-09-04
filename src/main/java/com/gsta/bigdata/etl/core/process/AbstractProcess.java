package com.gsta.bigdata.etl.core.process;

import java.util.List;
import java.util.Set;

import javax.xml.xpath.XPathExpressionException;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.AbstractETLObject;
import com.gsta.bigdata.etl.core.ChildrenTag;
import com.gsta.bigdata.etl.core.ComputingFrameworkConfigs;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.Context;
import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.etl.core.ShellContext;
import com.gsta.bigdata.etl.core.Transforms;
import com.gsta.bigdata.etl.core.source.AbstractSourceMetaData;
import com.gsta.bigdata.etl.core.source.InputPath;
import com.gsta.bigdata.etl.core.source.ValidatorException;
import com.gsta.bigdata.utils.StringUtils;
import com.gsta.bigdata.utils.XmlTools;

/**
 * abstract process
 * 
 * @author tianxq
 * 
 */
public abstract class AbstractProcess extends AbstractETLObject {
	@JsonProperty
	private String id;
	@JsonProperty
	private String type;
	@JsonProperty
	private ComputingFrameworkConfigs computingFrameworkConfigs;
	@JsonProperty
	private AbstractSourceMetaData sourceMetaData;
	@JsonProperty
	private Transforms transforms;
	@JsonProperty
	private ShellContext etlContext;

	public AbstractProcess() {
		super.tagName = Constants.PATH_PROCESS;

		super.registerChildrenTags(new ChildrenTag(
				Constants.PATH_COMPUTING_FRAMEWORK_CONFIGS, ChildrenTag.NODE));
		super.registerChildrenTags(new ChildrenTag(
				Constants.PATH_SOURCE_METADATA, ChildrenTag.NODE));
		super.registerChildrenTags(new ChildrenTag(Constants.PATH_TRANSFORMS,
				ChildrenTag.NODE));
	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		this.id = super.getAttr(Constants.ATTR_ID);
		if (this.id == null) {
			throw new ParseException("process id is null");
		}

		this.type = super.getAttr(Constants.ATTR_TYPE);
		this.type = StringUtils.upperCaseFirstChar(this.type);
		// default computing framework is map/reduce
		if (this.type == null || "".equals(this.type)) {
			this.type = Constants.DEFAULT_COMPUTING_FRAMEWORK_MR;
		}
	}

	@Override
	protected void createChildNode(Element node) throws ParseException {
		Preconditions.checkNotNull(node, "element  is null");

		if (node.getNodeName().equals(
				Constants.PATH_COMPUTING_FRAMEWORK_CONFIGS)) {
			this.computingFrameworkConfigs = new ComputingFrameworkConfigs();
			this.computingFrameworkConfigs.init(node);
		} else if (node.getNodeName().equals(Constants.PATH_SOURCE_METADATA)) {
			this.sourceMetaData = AbstractSourceMetaData.newInstance(node);
			this.sourceMetaData.init(node);
		} else if (node.getNodeName().equals(Constants.PATH_TRANSFORMS)) {
			this.transforms = new Transforms();
			this.transforms.init(node);
		}
	}

	@Override
	protected void createChildNodeList(NodeList nodeList) throws ParseException {
		// has no child node list
	}

	/**
	 * get computing framework configs<K,V>
	 * 
	 * @param key
	 * @return
	 */
	@JsonIgnore
	public String getConf(String key) {
		if (this.computingFrameworkConfigs == null) {
			return null;
		}

		return this.computingFrameworkConfigs.getCFConf(key);
	}

	/**
	 * get computing framework configs<K,V>
	 * 
	 * @param key
	 * @param defaultValue
	 * @return if value is null,return defaultValue
	 */
	@JsonIgnore
	public String getConf(String key, String defaultValue) {
		if (this.computingFrameworkConfigs == null) {
			return defaultValue;
		}

		return this.computingFrameworkConfigs.getCFConf(key, defaultValue);
	}

	/**
	 * get etl output value
	 * 
	 * @param data
	 * @return
	 */
	@JsonIgnore
	public abstract String getOutputValue(ETLData data) throws ETLException;

	/**
	 * get source file input paths
	 * 
	 * @return
	 */
	@JsonIgnore
	public List<InputPath> getInputPaths() {
		if (this.sourceMetaData == null) {
			return null;
		}

		return this.sourceMetaData.getInputPaths();
	}

	/**
	 * parse data file line to map<k,v> key=defined field in source meta data
	 * value=data line from data file
	 * 
	 * @param line
	 * @param invalidRecords
	 * @return
	 * @throws ETLException
	 */
	public ETLData parseLine(String line, Set<String> invalidRecords)
			throws ETLException, ValidatorException {
		if (this.sourceMetaData == null) {
			throw new ETLException(ETLException.NULL_SOURCE_META,"sourceMetaData object is null.");
		}

		return this.sourceMetaData.parseLine(line, invalidRecords);
	}
	
	/**
	 * verify source field,if want to keep original error record,
	 * please set it to invalidRecords object
	 * @param data
	 * @param line
	 * @param invalidRecords
	 * @throws ValidatorException
	 */
	public void verifyFields(ETLData data,String line,Set<String> invalidRecords) throws ValidatorException{
		if (this.sourceMetaData == null) {
			return;
		}
		
		this.sourceMetaData.verifyFields(data, line, invalidRecords);
	}
	
	/**
	 * verify source filed,if has error,throw exception
	 * @param data
	 * @throws ValidatorException
	 */
	public void verifyFields(ETLData data) throws ValidatorException{
		if (this.sourceMetaData == null) {
			return;
		}
		
		this.sourceMetaData.verifyFields(data);
	}

	/**
	 * execute etl transform
	 * 
	 * @param data
	 *            - source file data line
	 * @param scope
	 *            - transform scope,ex:map/reduce/others,default is map
	 */
	public void onTransform(ETLData data, String scope)
			throws ETLException {
		if (this.transforms == null) {
			// throw new ETLException("transforms object is null.");
			// if has no transforms tag,don't transform
			return;
		}

		this.transforms.onTransform(data, scope, etlContext);
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("process id=").append(this.id);

		sb.append("\r\ntype=").append(this.type);

		if (this.computingFrameworkConfigs != null) {
			sb.append("\r\ncomputingFrameConfigs:").append(
					this.computingFrameworkConfigs.toString());
		}
		if (this.sourceMetaData != null) {
			sb.append("\r\nsourceMetaData:\r\n").append(
					this.sourceMetaData.toString());
		}
		if (this.transforms != null) {
			sb.append("\r\ntransforms:\r\n").append(this.transforms.toString());
		}

		return sb.toString();
	}

	public String getId() {
		return id;
	}

	public String getType() {
		return type;
	}

	@JsonIgnore
	public abstract String getOutputPath();

	@JsonIgnore
	public abstract String getErrorPath();

	public void setContext(ShellContext context) {
		this.etlContext = context;
	}

	@JsonIgnore
	public String getSourceType() {
		if (this.sourceMetaData == null) {
			return null;
		}

		return this.sourceMetaData.getType();
	}

	public static AbstractProcess newInstance(Element element)
			throws ParseException {
		Preconditions.checkNotNull(element, "element  is null");

		AbstractProcess process = null;
		try {
			String type = XmlTools.getNodeAttr(element, Constants.ATTR_TYPE);

			type = Context.getValue(type);
			if (type == null || "".equals(type)) {
				type = Constants.DEFAULT_COMPUTING_FRAMEWORK_MR;
			}

			String subClassName = StringUtils
					.getPackageName(AbstractProcess.class)
					+ StringUtils.upperCaseFirstChar(type);

			process = (AbstractProcess) Class.forName(subClassName)
					.newInstance();
		} catch (ClassNotFoundException | InstantiationException
				| IllegalAccessException | XPathExpressionException e) {
			throw new ParseException(e);
		}

		return process;
	}
}

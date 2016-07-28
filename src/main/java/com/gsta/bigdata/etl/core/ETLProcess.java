package com.gsta.bigdata.etl.core;

import java.util.List;
import java.util.Set;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.source.AbstractSourceMetaData;
import com.gsta.bigdata.etl.core.source.InputPath;
import com.gsta.bigdata.etl.core.source.ValidatorException;
import com.gsta.bigdata.utils.StringUtils;

/**
 * etl process
 * 
 * @author tianxq
 * 
 */
public class ETLProcess extends AbstractETLObject {
	private static final long serialVersionUID = -8390198485290657527L;
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
	@JsonProperty
	private OutputMetaData outputMetaData;

	public ETLProcess() {
		super.tagName = Constants.PATH_PROCESS;

		super.registerChildrenTags(new ChildrenTag(
				Constants.PATH_DPI_FUNCTIONRULES, ChildrenTag.NODE));
		super.registerChildrenTags(new ChildrenTag(
				Constants.PATH_COMPUTING_FRAMEWORK_CONFIGS, ChildrenTag.NODE));
		super.registerChildrenTags(new ChildrenTag(
				Constants.PATH_SOURCE_METADATA, ChildrenTag.NODE));
		super.registerChildrenTags(new ChildrenTag(Constants.PATH_TRANSFORMS,
				ChildrenTag.NODE));
		super.registerChildrenTags(new ChildrenTag(
				Constants.PATH_OUTPUT_METADATA, ChildrenTag.NODE));
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
			if (this.sourceMetaData != null) {
				this.sourceMetaData.init(node);
			}
		} else if (node.getNodeName().equals(Constants.PATH_TRANSFORMS)) {
			this.transforms = new Transforms();
			this.transforms.init(node);
		}else if (node.getNodeName().equals(Constants.PATH_OUTPUT_METADATA)) {
			this.outputMetaData = new OutputMetaData();
			this.outputMetaData.init(node);
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
	//@JsonIgnore
	//public abstract String getOutputValue(ETLData data) throws ETLException;

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
	
	public List<ETLData> parseLine(String line)
			throws ETLException, ValidatorException {
		if (this.sourceMetaData == null) {
			throw new ETLException(ETLException.NULL_SOURCE_META,"sourceMetaData object is null.");
		}

		return this.sourceMetaData.parseLine(line);
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
	 */
	public void onTransform(ETLData data) throws TransformException {
		if (this.transforms == null) {
			// throw new ETLException("transforms object is null.");
			// if has no transforms tag,don't transform
			return;
		}

		this.transforms.onTransform(data,etlContext);
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
		if(this.outputMetaData != null){
			sb.append("\r\noutputMetaData:\r\n").append(this.outputMetaData.toString());
		}

		return sb.toString();
	}

	public String getId() {
		return id;
	}

	public String getType() {
		return type;
	}

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
	
	@JsonIgnore
	public String getOutputValue(ETLData data) throws ETLException{
		if (this.outputMetaData == null) {
			return null;
		}

		return this.outputMetaData.getOutputValue(data);
	}
	
	@JsonIgnore
	public String getOutputKey(ETLData data) throws ETLException {
		if (this.outputMetaData == null) {
			return null;
		}

		return this.outputMetaData.getOutputKey(data);
	}

	@JsonIgnore
	public String getOutputPath() {
		if (this.outputMetaData != null) {
			return this.outputMetaData.getOutputPath();
		}

		return null;
	}
	
	public String getOutputKafkaBrokers(){
		if (this.outputMetaData != null) {
			return this.outputMetaData.getBrokers();
		}

		return null;
	}
	
	public String getOutputKafkaTopic(){
		if (this.outputMetaData != null) {
			return this.outputMetaData.getTopic();
		}

		return null;
	}

	@JsonIgnore
	public String getErrorPath() {
		if (this.outputMetaData != null) {
			return this.outputMetaData.getErrorPath();
		}

		return null;
	}
	
	@JsonIgnore
	public String getOutputFilePrefix(){
		if(this.outputMetaData != null){
			return this.outputMetaData.getFilePrefix();
		}
		
		return null;
	}
	
	@JsonIgnore
	public String getOutputFileSuffix(){
		if(this.outputMetaData != null){
			return this.outputMetaData.getFileSuffix();
		}
		
		return null;
	}
	
	@JsonIgnore
	/**
	 * mode=0,single parse;mode=1,multi parse
	 * @return
	 */
	public int getMode() {
		if (this.computingFrameworkConfigs != null) {
			String mapperClass = this.computingFrameworkConfigs.getCFConf(Constants.HADOOP_MAPPER_CLASS);
			if (mapperClass != null && mapperClass.contains("MultiETLMapper")) {
				return 1;
			}
			return 0;
		}

		return -1;
	}

	public OutputMetaData getOutputMetaData() {
		return this.outputMetaData;
	}

	public AbstractSourceMetaData getSourceMetaData() {
		return sourceMetaData;
	}

}

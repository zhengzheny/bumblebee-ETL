package com.gsta.bigdata.etl.core.source;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.xml.xpath.XPathExpressionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.AbstractETLObject;
import com.gsta.bigdata.etl.core.ChildrenTag;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ContextMgr;
import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.Field;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.utils.StringUtils;
import com.gsta.bigdata.utils.XmlTools;

/**
 * abstract source file meta data define
 * 
 * @author tianxq
 * 
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
@JsonSubTypes({
		@JsonSubTypes.Type(value = SimpleFlat.class, name = "simpleFlat"),
		@JsonSubTypes.Type(value = NoSettingFieldsFlat.class, name = "noSettingFieldsFlat"),
		@JsonSubTypes.Type(value = FixedLengthFlat.class, name = "fixedLengthFlat"),
		@JsonSubTypes.Type(value = PgwXML.class, name = "pgwXML"),
		@JsonSubTypes.Type(value = FixedLengthByLineFlat.class, name = "fixedLengthByLineFlat") })
public abstract class AbstractSourceMetaData extends AbstractETLObject {
	@JsonProperty
	private String type;
	@JsonProperty
	private String wrapper;
	@JsonProperty
	private List<InputPath> inputPaths = new ArrayList<InputPath>();
	@JsonProperty
	private List<Field> fields = new ArrayList<Field>();

	private static Logger logger = LoggerFactory.getLogger(AbstractSourceMetaData.class);

	public AbstractSourceMetaData() {
		super.tagName = Constants.PATH_SOURCE_METADATA;

		super.registerChildrenTags(new ChildrenTag(
				Constants.PATH_SOURCE_METADATA_INPUT_PATHS,
				ChildrenTag.NODE_LIST));

		super.registerChildrenTags(new ChildrenTag(
				Constants.PATH_SOURCE_METADATA_FIELDS, ChildrenTag.NODE_LIST));
	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		this.type = super.getAttr(Constants.ATTR_TYPE);
		// default source file is simpleFlat type
		if (null == this.type || "".equals(this.type)) {
			type = Constants.DEFAULT_SOURCE_METADATA_FLAT;
		}

		this.wrapper = super.getAttr(Constants.ATTR_WRAPPER);
	}

	@Override
	protected void createChildNode(Element node) throws ParseException {
		// has no child node
	}

	@Override
	protected void createChildNodeList(NodeList nodeList) throws ParseException {
		Preconditions.checkNotNull(nodeList, "element is null");

		for (int i = 0; i < nodeList.getLength(); i++) {
			Node element = nodeList.item(i);
			if (element.getNodeType() == Node.ELEMENT_NODE) {
				if (element.getNodeName().equals(Constants.TAG_INPUTPATH)) {
					// parse source input path	
					InputPath inputPath = new InputPath();
					inputPath.init((Element) element);
					this.inputPaths.add(inputPath);
				} else if (element.getNodeName().equals(Constants.TAG_FIELD)) {
					// parse source fields
					Field field = new Field();
					field.init((Element) element);
					this.fields.add(field);
				}
			} // end if nodeType
		} // end for

		// sort List<Field> according to field's index
		// sometimes get Field from data line,allow blank <Field> when
		// initializing
		if (this.fields.size() > 0) {
			Collections.sort(this.fields);
		}
	}

	/**
	 * parse data file line to Map<key,value> key=source field definition
	 * value=data file field
	 * 
	 * @param line
	 * @param invalidRecords
	 * @return
	 */
	public abstract ETLData parseLine(String line, Set<String> invalidRecords)
			throws ETLException, ValidatorException;

	public static AbstractSourceMetaData newInstance(Element element)
			throws ParseException {
		Preconditions.checkNotNull(element, "element  is null");

		AbstractSourceMetaData sourceMetaData = null;
		try {
			String type = XmlTools.getNodeAttr(element, Constants.ATTR_TYPE);

			type = ContextMgr.getValue(type);
			if (type == null || "".equals(type)) {
				type = Constants.DEFAULT_SOURCE_METADATA_FLAT;
			}

			String subClassName = StringUtils
					.getPackageName(AbstractSourceMetaData.class)
					+ StringUtils.upperCaseFirstChar(type);

			sourceMetaData = (AbstractSourceMetaData) Class.forName(
					subClassName).newInstance();
		}catch(ClassNotFoundException | InstantiationException
				| IllegalAccessException | XPathExpressionException e){
			throw new ParseException(e);
		}

		return sourceMetaData;
	}

	public List<InputPath> getInputPaths() {
		return inputPaths;
	}

	public String getType() {
		return type;
	}

	public List<Field> getFields() {
		return fields;
	}

	public String getWrapper() {
		return wrapper;
	}
	
	public Field getFieldById(String id){
		if(id == null){
			return null;
		}
		
		Field retField = null;
		for(Field field:this.fields){
			if(id.equals(field.getId())){
				retField = field;
				break;
			}
		}
		return retField;
	}

	/**
	 * validate field
	 * 
	 * @param field
	 * @param data
	 * @param line
	 * @param invalidRecords
	 * @throws ValidatorException
	 */
	protected void fieldValidate(Field field, String data, String line,
			Set<String> invalidRecords) throws ValidatorException {
		Preconditions.checkNotNull(field, "field  is null");
		Preconditions.checkNotNull(invalidRecords, "invalidRecords  is null");

		// check data field
		try {
			field.validate(data);
		} catch (ValidatorException e) {
			// if strict check field,throw exception,
			// write invalid file and don't make transform
			if (field.isStrictCheck()) {
				throw e;
			} else {
				// if not strict check field,write invalid file and go on to
				// transform
				logger.error("dataline=" + line + ",error:" + e.getMessage());
				if (invalidRecords != null) {
					invalidRecords.add(line);
				}
			}
		}
	}

	/**
	 * verify etl data,if field's attribute strictCheck=false,
	 * it will not throw exception and write original line to invalid records 
	 * @param data
	 * @param line
	 * @param invalidRecords
	 * @throws ValidatorException
	 */
	public void verifyFields(ETLData data,String line, Set<String> invalidRecords)
			throws ValidatorException {
		Preconditions.checkNotNull(data, "ETLData  is null");
		Preconditions.checkNotNull(invalidRecords, "invalidRecords  is null");

		for (Field field : this.fields) {
			String fieldName = field.getId();
			String fieldValue = data.getData().get(fieldName);
			
			// check data field
			try {
				field.validate(fieldValue);
			} catch (ValidatorException e) {
				// if strict check field,throw exception,
				// write invalid file and don't make transform
				if (field.isStrictCheck()) {
					throw e;
				} else {
					// if not strict check field,write invalid file and go on to
					// transform
					logger.error("dataline=" + line + ",field=" + fieldName 
							+ e.getMessage());
					if (invalidRecords != null) {
						invalidRecords.add(line);
					}
				}
			}//end try
		}
	}
	
	public void verifyFields(ETLData data) throws ValidatorException {
		Preconditions.checkNotNull(data, "ETLData  is null");

		for (Field field : this.fields) {
			String fieldName = field.getId();
			String fieldValue = data.getData().get(fieldName);
			
			field.validate(fieldValue);
		}
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("type=").append(this.type);
		sb.append("\r\ninputPath=").append(this.inputPaths.toString());
		sb.append("\r\nfield=").append(this.fields.toString());

		return sb.toString();
	}
}

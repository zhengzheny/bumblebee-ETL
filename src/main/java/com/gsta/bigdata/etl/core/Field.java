package com.gsta.bigdata.etl.core;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.commons.validator.GenericValidator;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.gsta.bigdata.etl.core.source.ValidatorException;
import com.gsta.bigdata.utils.DataValidator;
import com.gsta.bigdata.utils.StringUtils;

/**
 * source and output field definition.
 * user can set index attribute for field's sorting
 * @author tianxq
 *
 */
public class Field extends AbstractETLObject implements Comparable<Field> {
	private static final long serialVersionUID = 2041977685658126961L;
	@JsonProperty
	private String id;
	@JsonProperty
	private String desc;
	@JsonProperty
	private int index = -1;
	@JsonProperty
	private String type;
	@JsonProperty
	private boolean notNull = false;
	/**
	 * if strick check is true,where verify invalid data,
	 * write invalid data file and don't make transform.
	 * or write invalid data file and go on making transform.
	 */
	@JsonProperty
	private boolean strictCheck = false;
	@JsonProperty
	private int minLength = -1;
	@JsonProperty
	private int maxLength = -1;
	@JsonProperty
	private int beginPos = -1;
	@JsonProperty
	private int endPos = -1;
	@JsonProperty
	private int length = -1;
	@JsonProperty
	private String defaultValue = null;
	
	private static final String[] TYPE_LIST = { "byte", "creditCard", "double",
			"email", "float", "int", "long", "short", "string", "idCard" };
	
	public Field() {
		super.tagName = Constants.TAG_FIELD;
		
		//has no child,don't register child tag
	}
	
	public Field(String id){
		super();
		this.id = id;
	}

	public Field(String id, String desc, int index, String type) {
		super();
		
		this.id = id;
		this.desc = desc;
		this.index = index;
		this.type = type;
	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		Preconditions.checkNotNull(element, "element is null");

		this.id = super.getAttr(Constants.ATTR_ID);
		this.desc = super.getAttr(Constants.ATTR_DESC);
		
		String strIndex = super.getAttr(Constants.ATTR_INDEX);
		if (null == strIndex || "".equals(strIndex)) {
			this.index = -1;
		} else {
			this.index = Integer.parseInt(strIndex);
		}

		this.type = super.getAttr(Constants.ATTR_TYPE);
		//default type is string
		if(this.type == null || "".equals(this.type)){
			this.type = "string";
		}else{
			//if type is set value,check type list
			boolean flag = false;
			for (String strType : TYPE_LIST) {
				if (strType.equals(this.type)) {
					flag = true;
					break;
				}
			}

			if (!flag) {
				throw new ParseException("invalid field type:" + this.type +
						",and you can only set type:" + TYPE_LIST.toString());	
			}
		}
		
		String strNotNull = super.getAttr(Constants.ATTR_NOT_NULL);
		if(strNotNull != null && "yes".equals(strNotNull)){
			this.notNull = true;
		}
		
		String strStrictCheck = super.getAttr(Constants.ATTR_STRICT_CHECK);
		if(strStrictCheck != null && "yes".equals(strStrictCheck)){
			this.strictCheck = true;
		}
		
		String strMinLength = super.getAttr(Constants.ATTR_MIN_LENGTH);
		if(strMinLength != null && !"".equals(strMinLength)){
			this.minLength = Integer.parseInt(strMinLength);
		}
		
		String strMaxLength = super.getAttr(Constants.ATTR_MAX_LENGTH);
		if(strMaxLength != null && !"".equals(strMaxLength)){
			this.maxLength = Integer.parseInt(strMaxLength);
		}
		
		String strLength = super.getAttr(Constants.ATTR_LENGTH);
		if(strLength != null && !"".equals(strLength)){
			this.length = Integer.parseInt(strLength);
		}
		
		String strDefaultValue = super.getAttr(Constants.ATTR_DEFAULT_VALUE);
		if(strDefaultValue != null && !"".equals(strDefaultValue)){
			this.defaultValue = ContextMgr.getValue(strDefaultValue);
		}
	}

	@Override
	protected void createChildNode(Element node) throws ParseException {
		//has no child
		
	}

	@Override
	protected void createChildNodeList(NodeList nodeList) throws ParseException {
		// has no childlist
		
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		
		if (obj == null) {
			return false;
		}
		
		if (getClass() != obj.getClass()) {
			return false;
		}
		
		Field other = (Field) obj;
		if (!this.id.equals(other.getId())) {
			return false;
		}
		
		return true;
	}
	
	public String getId() {
		return id;
	}

	public String getDesc() {
		return desc;
	}

	public int getIndex() {
		return index;
	}

	public String getType() {
		return type;
	}
	
	public int getBeginPos() {
		return beginPos;
	}

	public int getEndPos() {
		return endPos;
	}

	public int getLength() {
		return length;
	}

	public void setBeginPos(int beginPos) {
		this.beginPos = beginPos;
	}

	public void setEndPos(int endPos) {
		this.endPos = endPos;
	}
	
	public boolean isStrictCheck() {
		return strictCheck;
	}

	public String getDefaultValue() {
		return defaultValue;
	}

	@Override
	public int compareTo(Field field) {
		return this.index - field.getIndex();
	}
	
	/**
	 * verify data field is valid
	 * @param value
	 * @throws ValidatorException
	 */
	public void validate(String value) throws ValidatorException {
		if(value == null){
			throw new ValidatorException("value is null.");
		}
		
		//verify not null
		if(this.notNull){
			if(GenericValidator.isBlankOrNull(value)){
				throw new ValidatorException(ValidatorException.NULL_FIELD_NAMES,"field=" + this.id + 
						",the value is null");
			}
		}
		
		//verify field type
		//if add new type,only add TYPE_LIST and add function in DataValidator class
		try {
			String func = "is" + StringUtils.upperCaseFirstChar(this.type);			
			Method method = DataValidator.class.getDeclaredMethod(func,String.class);
			boolean ret = (boolean)method.invoke(DataValidator.class.newInstance(),value);
			if(!ret){
				throw new ValidatorException(ValidatorException.TYPE_NOT_MATCH,"field=" + this.id + 
						",must have " + this.type + " type,but value=" +
						value + " is not right.");
			}
		} catch (NoSuchMethodException | SecurityException
				| IllegalAccessException | IllegalArgumentException
				| InvocationTargetException | InstantiationException e) {
			throw new ValidatorException(ValidatorException.OTHER_EXCEPTION,e.getMessage());
		}

		// verify min length and max length
		if (this.minLength != -1
				&& !GenericValidator.minLength(value, this.minLength)) {
			throw new ValidatorException(ValidatorException.FIELD_MIN_LENGTH,"field=" + this.id
					+ ",must have min length=" + this.minLength
					+ ",but value length=" + value.length());
		}

		if (this.maxLength != -1
				&& !GenericValidator.maxLength(value, this.maxLength)) {
			throw new ValidatorException(ValidatorException.FIELD_MAX_LENGTH,"field=" + this.id
					+ ",must have max length=" + this.maxLength
					+ ",but value length=" + value.length());
		}
	}
}

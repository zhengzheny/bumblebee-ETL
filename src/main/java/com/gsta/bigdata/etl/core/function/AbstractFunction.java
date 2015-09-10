package com.gsta.bigdata.etl.core.function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.xpath.XPathExpressionException;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.AbstractETLObject;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.Context;
import com.gsta.bigdata.etl.core.ShellContext;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.utils.StringUtils;
import com.gsta.bigdata.utils.XmlTools;

/**
 * abstract function
 * 
 * @author tianxq
 * 
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
@JsonSubTypes({ @JsonSubTypes.Type(value = IP2Long.class, name = "IP2Long"),
		@JsonSubTypes.Type(value = Long2IP.class, name = "Long2IP"),
		@JsonSubTypes.Type(value = IsHoliday.class, name = "IsHoliday"),
		@JsonSubTypes.Type(value = DelWrapper.class, name = "DelWrapper"),
		@JsonSubTypes.Type(value = RedisSet.class, name = "RedisSet"),
		@JsonSubTypes.Type(value = RedisHSet.class, name = "RedisHSet"),
		@JsonSubTypes.Type(value = DimensionQuery.class, name = "DimensionQuery"),
		@JsonSubTypes.Type(value = HostQuery.class, name = "HostQuery")})
public abstract class AbstractFunction extends AbstractETLObject {
	@JsonProperty
	private List<String> inputs = new ArrayList<String>();
	@JsonProperty
	private List<String> outputs = new ArrayList<String>();
	@JsonProperty
	private String strInput;
	
	public AbstractFunction() {
		super.tagName = Constants.PATH_TRANSFORM_FUNCTION;
	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		this.strInput = super.getAttr(Constants.ATTR_INPUT);
		String strOutput = super.getAttr(Constants.ATTR_OUTPUT);

		// must have input or output
		if ((null == this.strInput || "".equals(this.strInput))
				&& (null == strOutput || "".equals(strOutput))) {
			throw new ParseException(
					"function must has input or output,or both are.");
		}

		if (null != strInput) {
			String[] fields = strInput.split(",");
			for (String field : fields) {
				this.inputs.add(field);
			}
		}

		if (null != strOutput) {
			String[] fields = strOutput.split(",");
			for (String field : fields) {
				this.outputs.add(field);
			}
		}
	}

	@Override
	protected void createChildNode(Element node) throws ParseException {
		// has no child node
	}

	@Override
	protected void createChildNodeList(NodeList nodeList) throws ParseException {
		// has no child node list
	}

	/**
	 * 
	 * @param data
	 * @param context
	 */
	public void getResult(Map<String, String> data, ShellContext context) {
		Map<String, String> functionData = new HashMap<String, String>();

		// avoid destroy data line,use function data
		functionData.putAll(data);

		/*
		 * function mode: 1.function has no input,output maybe has one or more
		 * field. <function name="sum" addend="f1" augend="f2" output="f3"/>
		 * <function name="isHoliday" output="ip,f2"/> calculate only once,write
		 * output to data line,
		 * 
		 * 2.function has one input which has one field. <function
		 * name="ip2long" input="f1" output="ip,f2"/> <function name="ip2long"
		 * input="f1" /> calculate only once,if has output,write output to data
		 * line, if has no output,reset input field and write it to data line.
		 * 
		 * 3.function has input which has many field,it must has no output
		 * <function name="delWrapper" input="*"/> <function name="delWrapper"
		 * input="_1,_2"/> calculate many once,and reset input field every time.
		 */

		// has only input field,maybe it's one field ,or *
		if (this.inputs.size() == 1) {
			// all field
			if ("*".equals(this.strInput)) {
				// read input data from data line's fields(all field)
				Set<String> setFields = data.keySet();
				Iterator<String> fields = setFields.iterator();
				while (fields.hasNext()) {
					String field = fields.next();
					String value = functionData.get(field);

					// input field="*" to function data,
					// subclass can get input field according input attribute in
					// configure file
					functionData.put(this.strInput, value);

					String result = this.onCalculate(functionData, context);

					// reset input field,ignore output field
					data.put(field, result);
				}
			} else {
				// only one input field
				String inputField = this.inputs.get(0);
				String result = this.onCalculate(functionData, context);
				// if have outputs,write output to data line
				if (this.outputs.size() > 0) {
					this.writeOutput(result, data);
				} else {
					// if has no output,reset input field
					data.put(inputField, result);
				}
			}
		} else if (this.inputs.size() > 1) {
			// multiple field
			Iterator<String> iter = this.inputs.iterator();
			while (iter.hasNext()) {
				String field = iter.next();
				String value = functionData.get(field);

				// put multiple field to data,ex:k=t1,t2,value=192.168.1.1
				// subclass can get input field according input attribute in
				// configure file
				functionData.put(this.strInput, value);

				String result = this.onCalculate(functionData, context);

				// reset input field,ignore output field
				data.put(field, result);
			}
		} else {
			// has no input attribute,only calculate once and put output to data
			// line
			String result = this.onCalculate(functionData, context);
			this.writeOutput(result, data);
		}
	}

	private void writeOutput(String result, Map<String, String> data) {
		Iterator<String> iter = this.outputs.iterator();
		while (iter.hasNext()) {
			String field = iter.next();
			data.put(field, result);
		}
	}

	/**
	 * function calculate don't care output attribute,framework will put result
	 * to data line
	 * 
	 * @param functionData
	 *            - include all data line's field value and input value; get
	 * functionData by field's name,not attribute's name; when input
	 * has 2 or more field,also get value by attribute name
	 * "input",this is Constants.ATTR_INPUT.
	 * @return
	 * @throws ETLException
	 */
	public abstract String onCalculate(Map<String, String> functionData,
			ShellContext context) throws ETLException;
	
	public abstract Map<String,String> multiOutputOnCalculate(Map<String, String> functionData,
			ShellContext context) throws ETLException;

	public String toString() {
		return super.getAttrs().toString();
	}

	public static AbstractFunction newInstance(Element element)
			throws ParseException {
		Preconditions.checkNotNull(element, "element  is null");

		AbstractFunction function = null;
		try {
			String name = XmlTools.getNodeAttr(element, Constants.ATTR_NAME);
			name = Context.getValue(name);
			if (name == null || "".equals(name)) {
				throw new ParseException("functon name is null.");
			}

			String subClassName = StringUtils
					.getPackageName(AbstractFunction.class)
					+ StringUtils.upperCaseFirstChar(name);

			function = (AbstractFunction) Class.forName(subClassName)
					.newInstance();
		} catch (ClassNotFoundException | InstantiationException
				| IllegalAccessException | XPathExpressionException e) {
			throw new ParseException(e);
		}

		return function;
	}
}

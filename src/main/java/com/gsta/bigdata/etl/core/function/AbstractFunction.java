package com.gsta.bigdata.etl.core.function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.xpath.XPathExpressionException;

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
		@JsonSubTypes.Type(value = ParseURL.class, name = "ParseURL"),
		@JsonSubTypes.Type(value = HostQuery.class, name = "HostQuery")})
public abstract class AbstractFunction extends AbstractETLObject {
	@JsonProperty
	private List<String> attrInputs = new ArrayList<String>();
	//output attribute for onCalculate computing model
	@JsonProperty
	private List<String> attrOutputs = new ArrayList<String>();
	@JsonProperty
	private String strInput;
	//output field id list for multiOutputOnCalculate computing model
	@JsonProperty
	private List<String> outputIds = new ArrayList<String>();
	
	public AbstractFunction() {
		super.tagName = Constants.PATH_TRANSFORM_FUNCTION;
		
		super.registerChildrenTags(new ChildrenTag(
				Constants.ATTR_OUTPUT,ChildrenTag.NODE_LIST));
	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		this.strInput = super.getAttr(Constants.ATTR_INPUT);
		String strOutput = super.getAttr(Constants.ATTR_OUTPUT);

		/*if ((null == this.strInput || "".equals(this.strInput))
				&& (null == strOutput || "".equals(strOutput))) {
			throw new ParseException(
					"function must has input or output,or both are.");
		}*/

		if (null != strInput) {
			String[] fields = strInput.split(",");
			for (String field : fields) {
				this.attrInputs.add(field);
			}
		}

		if (null != strOutput) {
			String[] fields = strOutput.split(",");
			for (String field : fields) {
				this.attrOutputs.add(field);
			}
		}
	}

	@Override
	protected void createChildNode(Element node) throws ParseException {
		// has no child node
	}

	@Override
	protected void createChildNodeList(NodeList nodeList) throws ParseException {
		Preconditions.checkNotNull(nodeList, "nodeList is null");

		for (int i = 0; i < nodeList.getLength(); i++) {
			Node element = nodeList.item(i);
			if (element.getNodeType() == Node.ELEMENT_NODE
					&& element.getNodeName().equals(Constants.ATTR_OUTPUT)) {
				try {
					String id = XmlTools.getNodeAttr((Element)element, Constants.ATTR_ID);
					this.outputIds.add(id);
				} catch (XPathExpressionException e) {
					throw new ParseException(e);
				}
			}
		}
	}

	/**
	 * transform get function calculate result 
	 * @param data - data from resource line
	 * @param context - shell context from shell command line
	 */
	public void getResult(Map<String, String> data, ShellContext context) {
		// avoid destroy data line,use function data on calculating
		Map<String, String> functionData = new HashMap<String, String>();
		functionData.putAll(data);
		
		/*if has multi output id like this:
		 * <function name="parseURL" input="url">
					<output id="urldomain" />
					<output id="urlhost" />
					<output id="urlpath" />
					<output id="urlquery" />
			</function>
		 * put calculate result to data and don't change input field and ignore output attribute field
		*/
		if (this.outputIds.size() > 0) {
			Map<String, String> ret = this.multiOutputOnCalculate(functionData, context);
			if(ret != null){
				data.putAll(ret);
			}
		} else {
			/*
			 * the function like this:
			 * <function name="long2IP" input="srcip" output="f1"/>
			 * function has input or output attribute
			 */
			this.getOnlyAttrComputing(functionData, data, context);
		}
	}
	
	/*
	 * function mode: 
	 * 1.function has no input,output maybe has one or more field. ex:
	 *     <function name="isHoliday" output="f1,f2"/> 
	 * calculate only once,write result to output field.
	 * 
	 * 2.function have one field input and one or more output field.ex:
	 *    <function name="ip2long" input="ip" output="f1,f2"/> 
	 * calculate only once,write result to output and don't change input field.
	 * 
	 * 3.function have one input which has one field and no output field.ex:
	 *    <function name="ip2long" input="ip"/> 
	 * calculate only once,change input field. 
	 *    
	 * 4.function has one input field which id is "*".ex:
	 * 	  <function name="delWrapper" input="*" wrapper="&quot;"/>
	 * calculate many once by source field,and reset every source field every time.
	 * even if defined output field,ignore them.
	 * 
	 * 5.function have many input field
	 *   <function name="delWrapper" input="f1,f2"/> 
	 * calculate many once by input's fields,and reset input's fields every time.
	 * even if defined output field,ignore them.
	 */
	private void getOnlyAttrComputing(Map<String, String> functionData,
			Map<String, String> data,ShellContext context){
		// has only input field,maybe it's one field ,or *
		if (this.attrInputs.size() == 1) {
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
				String inputField = this.attrInputs.get(0);
				String result = this.onCalculate(functionData, context);
				// if have outputs,write output to data line
				if (this.attrOutputs.size() > 0) {
					this.writeOutputAttr(result, data);
				} else {
					// if has no output,reset input field
					data.put(inputField, result);
				}
			}
		} else if (this.attrInputs.size() > 1) {
			// multiple field
			Iterator<String> iter = this.attrInputs.iterator();
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
			// has no input attribute,only calculate once and put output to result
			String result = this.onCalculate(functionData, context);
			this.writeOutputAttr(result, data);
		}
	}

	private void writeOutputAttr(String result, Map<String, String> data) {
		Iterator<String> iter = this.attrOutputs.iterator();
		while (iter.hasNext()) {
			String field = iter.next();
			data.put(field, result);
		}
	}

	/**
	 * function calculate don't care output attribute,framework will put result
	 * to data line
	 *  the function like this:
	 *      <function name="long2IP" input="srcip" output="f1"/>
	 * function has input or output attribute
	 * 
	 * @param functionData
	 * @param context
	 * @return
	 * @throws ETLException
	 */
	protected abstract String onCalculate(Map<String, String> functionData,
			ShellContext context) throws ETLException;
	
	/**
	 * if has multi output id like this:
	 * <function name="parseURL" input="url">
					<output id="urldomain" />
					<output id="urlhost" />
					<output id="urlpath" />
					<output id="urlquery" />
	 *	</function>
     * put calculate result to data and don't change input field and 
     * ignore output attribute field
	 * @param functionData
	 * @param context
	 * @return
	 * @throws ETLException
	 */
	protected abstract Map<String,String> multiOutputOnCalculate(Map<String, String> functionData,
			ShellContext context) throws ETLException;

	public List<String> getOutputIds() {
		return outputIds;
	}

	public static AbstractFunction newInstance(Element element)
			throws ParseException {
		Preconditions.checkNotNull(element, "element  is null");

		AbstractFunction function = null;
		try {
			String name = XmlTools.getNodeAttr(element, Constants.ATTR_NAME);
			name = ContextMgr.getValue(name);
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

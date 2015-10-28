package com.gsta.bigdata.etl.core.filter;

import java.util.HashMap;
import java.util.Map;

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
import com.gsta.bigdata.etl.core.ContextMgr;
import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.etl.core.ShellContext;
import com.gsta.bigdata.utils.StringUtils;
import com.gsta.bigdata.utils.XmlTools;

/**
 * abstract filter
 * @author tianxq
 *
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
@JsonSubTypes({ 
	@JsonSubTypes.Type(value = In.class, name = "in"),
	@JsonSubTypes.Type(value = DimensionCheck.class, name = "dimensionCheck")})
public abstract class AbstractFilter extends AbstractETLObject {
	private static final long serialVersionUID = -9175151443255964228L;
	@JsonProperty
	private String name;
	
	public AbstractFilter() {
		super.tagName = Constants.PATH_TRANSFORM_FILTER;
	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		this.name = super.getAttr(Constants.ATTR_NAME);
	}

	@Override
	protected void createChildNode(Element node) throws ParseException {
		//has no child node
	}

	@Override
	protected void createChildNodeList(NodeList nodeList) throws ParseException {
		//has no child node list
	}

	public boolean filter(ETLData data,ShellContext context){
		Map<String,String> filterData = new HashMap<String,String>();

		//avoid destroy data line,use filter data
		filterData.putAll(data.getData());
		
		return this.accept(filterData,context);
	}
	
	/**
	 * filter accept
	 * @param data
	 * @return
	 */
	protected abstract boolean accept(Map<String,String> filterData,ShellContext context)
			throws ETLException;

	public String getName() {
		return name;
	}

	public static AbstractFilter newInstance(Element element)
			throws ParseException {
		Preconditions.checkNotNull(element, "element  is null");

		AbstractFilter filter = null;
		try {
			String name = XmlTools.getNodeAttr(element, Constants.ATTR_NAME);

			name = ContextMgr.getValue(name);
			if (name == null || "".equals(name)) {
				throw new ParseException("filter name is null.");
			}

			String subClassName = StringUtils
					.getPackageName(AbstractFilter.class)
					+ StringUtils.upperCaseFirstChar(name);

			filter = (AbstractFilter)Class.forName(subClassName).newInstance();
		} catch (ClassNotFoundException | InstantiationException
				| IllegalAccessException | XPathExpressionException e) {
			throw new ParseException(e);
		}

		return filter;
	}
}

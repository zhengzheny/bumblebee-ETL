package com.gsta.bigdata.etl.core.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.w3c.dom.Element;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ShellContext;
import com.gsta.bigdata.etl.core.ParseException;

/**
 * filter demo
 * @author tianxq
 *
 */
public class In extends AbstractFilter {
	@JsonProperty
	private List<String> list = new ArrayList<String>();
	@JsonProperty
	private String inputField = null;
	
	public In() {
		super();
	}

	@Override
	public boolean accept(Map<String, String> filterData, ShellContext context)
			throws ETLException {
		String inputValue = filterData.get(this.inputField);

		if (this.list.contains(inputValue)) {
			return true;
		}

		return false;
	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		super.initAttrs(element);
		
		this.inputField = super.getAttr(Constants.ATTR_INPUT);
		
		String strList = super.getAttr(Constants.ATTR_LIST);
		if(strList != null){
			String[] tempList = strList.split(",");
			for(String str:tempList){
				this.list.add(str);
			}
		}
	}
}

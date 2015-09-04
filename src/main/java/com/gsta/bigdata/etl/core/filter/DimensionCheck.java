package com.gsta.bigdata.etl.core.filter;

import java.util.Map;

import org.w3c.dom.Element;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ShellContext;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.etl.core.lookup.ILookup;
import com.gsta.bigdata.etl.core.lookup.LKPTableMgr;

/**
 * dimension table filter
 * 
 * @author shine
 * 
 */
public class DimensionCheck extends AbstractFilter {
	@JsonProperty
	private ILookup table;
	@JsonProperty
	private String inputField = null;

	public DimensionCheck() {
		super();
	}

	@Override
	public boolean accept(Map<String, String> filterData, ShellContext context)
			throws ETLException {
		String inputValue = filterData.get(this.inputField);
		
		if (this.table.isExist(inputValue)) {
			return true;
		}

		return false;
	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		super.initAttrs(element);

		this.inputField = super.getAttr(Constants.ATTR_INPUT);

		String tableId = super.getAttr(Constants.ATTR_LOOKUP_TABLE);
		
		this.table = LKPTableMgr.getInstance().getTable(tableId);
		if(this.table == null){
 			throw new ParseException("there is no table " + tableId +" in configure file.");
 		}
	}
}

package com.gsta.bigdata.etl.core.function;

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
 * dimension table query
 * 
 * @author Shine
 * 
 */
public class DimensionQuery extends AbstractFunction {
	@JsonProperty
	private ILookup table;
 	@JsonProperty
 	private String inputField;
 	
	public DimensionQuery() {
		super();
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

	@Override
	public String onCalculate(Map<String, String> functionData,
			ShellContext context) throws ETLException {
 		String value = functionData.get(inputField);
 		
 		String queryValue = null;
 		if(value.indexOf("/r") != -1){
 			queryValue = this.table.getValue(value, true);
 		}else{
 			queryValue = this.table.getValue(value);
 		}
 		
 		return queryValue;
	}

}
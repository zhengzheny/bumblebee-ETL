package com.gsta.bigdata.etl.core.function;

import java.util.HashMap;
import java.util.Map;

import org.w3c.dom.Element;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.etl.core.ShellContext;
import com.gsta.bigdata.etl.core.lookup.DIMDS;
import com.gsta.bigdata.etl.core.lookup.ILookup;
import com.gsta.bigdata.etl.core.lookup.LKPTableMgr;
import com.gsta.bigdata.etl.core.source.mro.DIMObj;

public class DIMInfoQuery extends AbstractFunction {
	private static final long serialVersionUID = 5713845077173408518L;
	@JsonProperty
	protected ILookup table;
	@JsonProperty
	protected String[] keys;

	public DIMInfoQuery() {
		super();
	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		super.initAttrs(element);
		
		String str = super.getAttr(Constants.ATTR_KEY);
		this.keys = str.split(",", -1);
		
 		String tableId = super.getAttr(Constants.ATTR_LOOKUP_TABLE);		
 		this.table = LKPTableMgr.getInstance().getTable(tableId);
 		if(this.table == null){
 			throw new ParseException("there is no table " + tableId +" in configure file.");
 		}
	}

	@Override
	protected String onCalculate(Map<String, String> functionData,
			ShellContext context) throws ETLException {
		
		return null;
	}

	@Override
	protected Map<String, String> multiOutputOnCalculate(
			Map<String, String> functionData, ShellContext context)
			throws ETLException {
		Map<String, String> ret = new HashMap<String, String>();
		
		DIMObj dimObj = (DIMObj)this.table.getValue(DIMDS.getKey(functionData,this.keys));
		if(dimObj != null){
			ret.putAll(dimObj.getBasicInfo(super.getOutputIds()));
		}
		
		return ret;
	}
}

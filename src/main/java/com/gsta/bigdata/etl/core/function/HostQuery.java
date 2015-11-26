package com.gsta.bigdata.etl.core.function;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.w3c.dom.Element;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ShellContext;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.etl.core.lookup.LKPTable;
import com.gsta.bigdata.etl.core.lookup.LKPTableMgr;
import com.gsta.bigdata.utils.StringUtils;

/**
 * host query
 * the host from URL,lookup from dimension table and 
 * found the longest key/value which is contained by host
 * 
 * @author Shine
 * 
 */
public class HostQuery extends AbstractFunction {
	private static final long serialVersionUID = 7648551153488869583L;
	@JsonProperty
	private String inputField;
	@JsonProperty
	private Map<String, String> sortedMap = new LinkedHashMap<String, String>();

	public HostQuery() {
		super();
	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		super.initAttrs(element);

		this.inputField = super.getAttr(Constants.ATTR_INPUT);
		String tableId = super.getAttr(Constants.ATTR_LOOKUP_TABLE);

		LKPTable table = LKPTableMgr.getInstance().getTable(tableId);
		if (table == null) {
			throw new ParseException("there is no table " + tableId
					+ " in configure file.");
		}

		List<Map.Entry<String, String>> dimensionList = new ArrayList<Map.Entry<String, String>>();
		for (Map.Entry<String, String> entry : table.getDimensions()
				.entrySet()) {
			dimensionList.add(entry);
		}
		
		// order by key's length desc
		Collections.sort(dimensionList,
				new Comparator<Map.Entry<String, String>>() {
					@Override
					public int compare(Map.Entry<String, String> o1,
							Map.Entry<String, String> o2) {
						return o2.getKey().length() - o1.getKey().length();
					}
				});

		for (Map.Entry<String, String> entry : dimensionList) {
			this.sortedMap.put(entry.getKey(), entry.getValue());
		}
	}

	@Override
	protected String onCalculate(Map<String, String> functionData,
			ShellContext context) throws ETLException {
		String value = functionData.get(this.inputField);
		String host = StringUtils.getHost(value);
		
		String result = "";
		for (String key : this.sortedMap.keySet()) {
			if(null == key || "".equals(key)){
				continue;
			}
			
			if (host.contains(key)) {
				result = this.sortedMap.get(key);
				break;
			}
		}
		
		return result;
	}
	
	@Override
	protected Map<String, String> multiOutputOnCalculate(
			Map<String, String> functionData, ShellContext context)
			throws ETLException {
		return null;
	}
}
package com.gsta.bigdata.etl.core.source;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.w3c.dom.NodeList;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.Field;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.utils.StringUtils;

public class Vertical2Cross extends SimpleFlat {
	private static final long serialVersionUID = -8283470368142804579L;
	@JsonProperty
	protected List<String> masterKeys = new ArrayList<String>();
	
	public Vertical2Cross() {
		super();
	}

	@Override
	protected void createChildNodeList(NodeList nodeList) throws ParseException {
		super.createChildNodeList(nodeList);

		// save verify field
		Iterator<Field> iter = super.getFields().iterator();
		while (iter.hasNext()) {
			Field field = iter.next();
			if (field.isMasterKey()) {
				this.masterKeys.add(field.getId());
			}
		}
	}

	@Override
	public ETLData parseLine(String line, Set<String> invalidRecords)
			throws ETLException, ValidatorException {
		Preconditions.checkNotNull(line, "data line is null");

		String[] data = StringUtils.splitByWrapper(line, 
				super.getDelimiter(),super.getWrapper());
		
		if(data == null){
			throw new ETLException(ETLException.NULL_DATA_BY_SPLIT,"parse line data occur null,delimiter=" +
					super.getDelimiter() + ",wrapper=" + super.getWrapper());
		}
		
		if (data.length != super.getFields().size()) {
			throw new ETLException(ETLException.DATA_NOT_EQUAL_DEFINITION,"data " + line + " £¬record count="
					+ data.length + ",but source definition field count="
					+ super.getFields().size());
		}
		
		ETLData etlData = new ETLData();
		String fieldName = null,fieldValue = null;
		for (int i = 0; i < data.length; i++) {
			Field field = super.getFields().get(i);
			if(field.getId().equals(Constants.FIELD_NAME)){
				fieldName = data[i];
			}else if(field.getId().equals(Constants.FIELD_VALUE)){
				fieldValue = data[i];
			}else{
				etlData.addData(field.getId(), data[i]);
			}
		}
		if(fieldName != null && fieldValue != null){
			etlData.addData(fieldName, fieldValue);
		}
		
		etlData.addData(Constants.FIELD_KEY,this.getMasterKey(etlData));
		
		return etlData;
	}
	
	protected String getMasterKey(ETLData etlData) {
		StringBuffer sb = new StringBuffer();

		for (int i = 0; i < this.masterKeys.size(); i++) {
			sb.append(etlData.getValue(this.masterKeys.get(i))).
			   append(Constants.DEFAULT_KEY_DELI);
		}

		return sb.toString();
	}

	@Override
	public List<ETLData> parseLine(String line) throws ETLException,
			ValidatorException {
		return null;
	}

}

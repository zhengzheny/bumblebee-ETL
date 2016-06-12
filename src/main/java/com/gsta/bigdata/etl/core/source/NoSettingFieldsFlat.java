package com.gsta.bigdata.etl.core.source;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.Field;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.utils.StringUtils;

/**
 * This source meta data means that user don't configure the source fields.
 * 
 * @author tianxq
 * 
 */
public class NoSettingFieldsFlat extends SimpleFlat {
	private static final long serialVersionUID = -292446003653175459L;
	@JsonProperty
	private String fieldNum;
	@JsonProperty
	private List<String> fieldIds = new ArrayList<String>();

	private static final String ATTR_FIELD_NUM = "fieldNum";
	@JsonProperty
	private int nums = 0;
	@JsonProperty
	private int operator = EQ;
	
	private static final int EQ = 0;
	private static final int GT = 1;
	private static final int LT = 2;
	private static final int GE = 3;
	private static final int LE = 4;

	public NoSettingFieldsFlat() {
		super();
	}

	@Override
	protected void createChildNodeList(NodeList nodeList) throws ParseException {
		super.createChildNodeList(nodeList);

		// save verify field
		Iterator<Field> iter = super.getFields().iterator();
		while (iter.hasNext()) {
			Field field = iter.next();

			this.fieldIds.add(field.getId());
		}
	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		super.initAttrs(element);

		this.fieldNum = super.getAttr(ATTR_FIELD_NUM);
		if(this.fieldNum == null || "".equals(this.fieldNum)){
			return;
		}
		
		try {
			this.nums = Integer.parseInt(StringUtils.getNumbers(this.fieldNum));
			String str = StringUtils.getNotNumbers(this.fieldNum).trim();
			switch (str) {
			case "gt":
				this.operator = GT;
				break;
			case "lt":
				this.operator = LT;
				break;
			case "ge":
				this.operator = GE;
				break;
			case "le":
				this.operator = LE;
				break;
			default:
				this.operator = EQ;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	/**
	 * transform data line to map<k,v>,
	 * k=_n(n is field index from data line),value=field's value.
	 * user can use _n in function configure,means the n field in data line
	 */
	public ETLData parseLine(String line, Set<String> invalidRecords)
			throws ETLException {
		Preconditions.checkNotNull(line, "data line is null");

		// String[] datas = line.split(super.getDelimiter(), -1);
		String[] datas = StringUtils.splitByWrapper(line, super.getDelimiter(),
				super.getWrapper());

		if (datas == null) {
			throw new ETLException(ETLException.NULL_DATA_BY_SPLIT,"parse line occur null,delimiter="
					+ super.getDelimiter() + ",wrapper=" + super.getWrapper());
		}

		if (null != this.fieldNum && !"".equals(this.fieldNum)) {
			switch(this.operator){
			case EQ:
				if (this.nums != datas.length) {
					throw new ETLException(ETLException.DATA_NOT_EQUAL_DEFINITION,"data " + line + " record count="
							+ datas.length + ",but source definition fieldNum="
							+ this.fieldNum);
				}
				break;
			case GT:
				if (this.nums <= datas.length) {
					throw new ETLException(ETLException.DATA_NOT_EQUAL_DEFINITION,
							"source definition fieldNum must greater than "
									+ nums + ",but source " + line
									+ " record count=" + datas.length);
				}
				break;
			case LT:
				if (this.nums >= datas.length) {
					throw new ETLException(ETLException.DATA_NOT_EQUAL_DEFINITION,
							"source definition fieldNum must less than "
									+ nums + ",but source " + line
									+ " record count=" + datas.length);
				}
				break;
			case GE:
				if (this.nums < datas.length) {
					throw new ETLException(ETLException.DATA_NOT_EQUAL_DEFINITION,
							"source definition fieldNum must greater and equal than "
									+ nums + ",but source " + line
									+ " record count=" + datas.length);
				}
				break;
			case LE:
				if (this.nums > datas.length) {
					throw new ETLException(ETLException.DATA_NOT_EQUAL_DEFINITION,
							"source definition fieldNum must less and equal than "
									+ nums + ",but source " + line
									+ " record count=" + datas.length);
				}
				break;
			}
		}

		ETLData etlData = new ETLData();
		int i = 1;

		for (String data : datas) {
			String fieldName = "_" + i;

			etlData.addData(fieldName, data);

			// check data field
			if (this.fieldIds.contains(fieldName)) {
				Field field = super.getFieldById(fieldName);
				super.fieldValidate(field, datas[i - 1], line, invalidRecords);
			}

			i++;
		} // end for

		return etlData;
	}
}

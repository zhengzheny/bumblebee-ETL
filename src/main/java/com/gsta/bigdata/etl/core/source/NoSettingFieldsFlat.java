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
	private static final String[] OPERATORS = { "gt", "lt", "ge", "le" };

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
			// if fieldNum attribute all digit,judge notequal
			if (StringUtils.isDigit(this.fieldNum)) {
				if (Integer.parseInt(this.fieldNum) != datas.length) {
					throw new ETLException(ETLException.DATA_NOT_EQUAL_DEFINITION,"data " + line + " record count="
							+ datas.length + ",but source definition fieldNum="
							+ this.fieldNum);
				}
			} else {
				String nums = StringUtils.getNumbers(this.fieldNum);
				String noNums = StringUtils.getNotNumbers(this.fieldNum).trim();
				// if operator is gt,remove less and equal filed numbers line
				if (noNums.equals(OPERATORS[0])) {
					if (datas.length <= Integer.parseInt(nums)) {
						throw new ETLException(ETLException.DATA_NOT_EQUAL_DEFINITION,
								"source definition fieldNum must greater than "
										+ nums + ",but source " + line
										+ " record count=" + datas.length);
					}
					// if operator is lt,remove greater and equal filed numbers
					// line
				} else if (noNums.equals(OPERATORS[1])) {
					if (datas.length >= Integer.parseInt(nums)) {
						throw new ETLException(ETLException.DATA_NOT_EQUAL_DEFINITION,
								"source definition fieldNum must less than "
										+ nums + ",but source " + line
										+ " record count=" + datas.length);
					}
					// if operator is ge,remove less filed numbers line
				} else if (noNums.equals(OPERATORS[2])) {
					if (datas.length < Integer.parseInt(nums)) {
						throw new ETLException(ETLException.DATA_NOT_EQUAL_DEFINITION,
								"source definition fieldNum must greater and equal than "
										+ nums + ",but source " + line
										+ " record count=" + datas.length);
					}
					// if operator is le,remove greater filed numbers line
				} else if (noNums.equals(OPERATORS[3])) {
					if (datas.length > Integer.parseInt(nums)) {
						throw new ETLException(ETLException.DATA_NOT_EQUAL_DEFINITION,
								"source definition fieldNum must less and equal than "
										+ nums + ",but source " + line
										+ " record count=" + datas.length);
					}
				} else {
					throw new ETLException(ETLException.DATA_NOT_EQUAL_DEFINITION,"fieldNum written error");
				}
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

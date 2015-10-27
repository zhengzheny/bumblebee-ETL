package com.gsta.bigdata.etl.core.source;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.Field;
import com.gsta.bigdata.etl.core.ParseException;

/**
 * parse zte xml by String
 * 
 * @author shine
 *
 */
public class ZteENODEBXML extends AbstractSourceMetaData {
	private String beginMeasData = "<measData>";

	private String beginTime;
	private ETLData etlData = new ETLData();
	private String[] types;

	@JsonProperty
	private List<String> fieldIds = new ArrayList<String>();

	// date format pattern
	private String oldPattern = "yyyy-MM-dd'T'HH:mm:ss";
	private String newPattern = "yyyyMMddHHmmss";

	private static final String ATTR_BEGINTIME = "beginTime";
	private static final String ATTR_MEASOBJLDN = "measObjLdn";

	private static final String TAG_MEASTYPES = "measTypes";
	private static final String TAG_MEASRESULTS = "measResults";

	private static final String FIELD_COLLECTTIME = "COLLECTTIME";
	private static final String FIELD_SBNID = "SBNID";
	private static final String FIELD_ENODEBID = "ENODEBID";
	private static final String FIELD_CELLID = "CellID";

	private Logger logger = LoggerFactory.getLogger(getClass());

	public ZteENODEBXML() {
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
	public ETLData parseLine(String line, Set<String> invalidRecords)
			throws ETLException, ValidatorException {
		if (line.contains(beginMeasData)) {
			this.etlData.clear();
		}

		if (StringUtils.isBlank(this.beginTime)) {
			this.beginTime = this.getAttrValue(line, ATTR_BEGINTIME);
			if (StringUtils.isNotBlank(this.beginTime)) {
				try {
					this.beginTime = com.gsta.bigdata.utils.StringUtils
							.dateFormat(this.beginTime, oldPattern, newPattern);
				} catch (java.text.ParseException e) {
					logger.error("beginTime:" + this.beginTime
							+ " format error");
				}
			}
		}

		String measObjLdn = this.getAttrValue(line, ATTR_MEASOBJLDN);
		this.splitMeasObjLdn(measObjLdn);

		String measTypes = this.getTagValue(line, TAG_MEASTYPES);
		if (StringUtils.isNotBlank(measTypes)) {
			this.types = measTypes.split(" ");
		}

		String measResults = this.getTagValue(line, TAG_MEASRESULTS);
		if (StringUtils.isNotBlank(measResults)) {
			String[] values = measResults.split(" ");
			if (this.types.length != values.length) {
				throw new ETLException("measObjLdn:" + measObjLdn
						+ ",measTypes size=" + this.types.length
						+ ",but measResults size=" + values.length);
			} else {
				for (int i = 0; i < this.types.length; i++) {
					etlData.addData(types[i], values[i]);
				}
			}
		}

		if (line.indexOf(TAG_MEASRESULTS) != -1) {
			this.etlData.addData(FIELD_COLLECTTIME, this.beginTime);

			if (this.fieldIds != null && this.fieldIds.size() > 0) {
				for (String fieldName : etlData.getFieldNames()) {
					// check data field
					if (this.fieldIds.contains(fieldName)) {
						Field field = super.getFieldById(fieldName);
						String fieldValue = etlData.getData().get(fieldName);
						super.fieldValidate(field, fieldValue, fieldValue,
								invalidRecords);
					}
				}
			}
			return etlData;
		}

		return null;
	}

	private void splitMeasObjLdn(String measObjLdn) {
		if (StringUtils.isBlank(measObjLdn)) {
			return;
		}

		String[] objLdns = measObjLdn.split(",");
		for (String objLdn : objLdns) {
			String[] obj = objLdn.split("=");
			if (FIELD_SBNID.equals(obj[0])) {
				this.etlData.addData(FIELD_SBNID, obj[1]);
			} else if (FIELD_ENODEBID.equals(obj[0])) {
				this.etlData.addData(FIELD_ENODEBID, obj[1]);
			} else if (FIELD_CELLID.equals(obj[0])) {
				this.etlData.addData(FIELD_CELLID, obj[1]);
			}
		}
	}

	private String getAttrValue(String str, String attrName) {
		if (StringUtils.isBlank(str) || StringUtils.isBlank(attrName)) {
			return null;
		}

		String ret = null;
		int index = str.indexOf(attrName);
		if (index != -1) {
			int begin = index + attrName.length() + 2;
			int end = str.lastIndexOf("\"");
			ret = str.substring(begin, end);
		}

		return ret;
	}

	private String getTagValue(String str, String tagName) {
		if (StringUtils.isBlank(str) || StringUtils.isBlank(tagName)) {
			return null;
		}

		String ret = null;
		int index = str.indexOf(tagName);
		if (index != -1) {
			int begin = index + tagName.length() + 1;
			int end = str.lastIndexOf("<");
			ret = str.substring(begin, end);
		}

		return ret;
	}

}

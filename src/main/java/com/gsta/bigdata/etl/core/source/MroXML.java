package com.gsta.bigdata.etl.core.source;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.Field;
import com.gsta.bigdata.etl.core.ParseException;

/**
 * parse MRO xml by String
 * 
 * @author shine
 *
 */
public class MroXML extends AbstractSourceMetaData {
	private static final long serialVersionUID = 4972621315058493751L;

	private String beginFileHeader = "<fileHeader";
	private String beginObject = "<object";
	private String beginV = "<v>";
	private String beginSmr = "<smr>";

	private String startTime;
	private String endTime;
	private String timeStamp;
	private String eNodeID;
	private String cellID;
	private String mmeGroupId;
	private String mmeUeS1apId;
	private String mmeCode;
	private List<String> smrs = new ArrayList<String>();

	private ETLData etlData = new ETLData();

	@JsonProperty
	private List<String> fieldIds = new ArrayList<String>();

	private static final String ATTR_STARTTIME = "startTime";
	private static final String ATTR_ENDTIME = "endTime";
	private static final String ATTR_TIMESTAMP = "TimeStamp";
	private static final String ATTR_ID = "id";
	private static final String ATTR_MMEGROUPID = "MmeGroupId";
	private static final String ATTR_MMEUES1APID = "MmeUeS1apId";
	private static final String ATTR_MMECODE = "MmeCode";

	private static final String TAG_SMR = "smr";
	private static final String TAG_V = "v";

	private static final String FIELD_STARTTIME = "startTime";
	private static final String FIELD_ENDTIME = "endTime";
	private static final String FIELD_TIMESTAMP = "TimeStamp";
	private static final String FIELD_ENODEBID = "ENODEID";
	private static final String FIELD_CELLID = "CELLID";
	private static final String FIELD_MMEGROUPID = "MmeGroupId";
	private static final String FIELD_MMEUES1APID = "MmeUeS1apId";
	private static final String FIELD_MMECODE = "MmeCode";

	public MroXML() {
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
		if (line.indexOf(beginFileHeader) != -1) {
			this.startTime = this.getAttrValue(line, ATTR_STARTTIME).replace(
					"T", " ");
			this.endTime = this.getAttrValue(line, ATTR_ENDTIME).replace("T",
					" ");
		}

		if (line.indexOf(beginObject) != -1) {
			this.etlData.clear();

			this.timeStamp = this.getAttrValue(line, ATTR_TIMESTAMP).replace(
					"T", " ");
			String id = this.getAttrValue(line, ATTR_ID).trim();
			if (StringUtils.isNotBlank(id)) {
				String[] ids = id.split("-");
				// mro-zte object id
				if (ids.length == 1) {
					try {
						this.eNodeID = String
								.valueOf(Integer.parseInt(id) / 256);
						this.cellID = String
								.valueOf(Integer.parseInt(id) % 256);
					} catch (NumberFormatException e) {
						throw new ETLException(ETLException.MRO_XML_ID_ERROR,
								"mro xml id is error,id=" + id);
					}
				}
				// mro-hw object id
				if (ids.length == 2) {
					this.eNodeID = ids[0];
					this.cellID = ids[1];
				}
			}

			this.mmeGroupId = this.getAttrValue(line, ATTR_MMEGROUPID);
			this.mmeUeS1apId = this.getAttrValue(line, ATTR_MMEUES1APID);
			this.mmeCode = this.getAttrValue(line, ATTR_MMECODE);
		}

		if (line.indexOf(beginSmr) != -1) {
			this.smrs.clear();

			String smr = this.getTagValue(line, TAG_SMR);
			if (StringUtils.isNotBlank(smr)) {
				String[] tempSmr = smr.replace(".", "_").split(" ");
				for (String temp : tempSmr) {
					this.smrs.add(temp);
				}
			}
		}

		if (line.indexOf(beginV) != -1) {
			String v = this.getTagValue(line, TAG_V);
			if (StringUtils.isNotBlank(v)) {
				String[] values = v.split(" ");
				if (this.smrs.size() != values.length) {
					throw new ETLException(ETLException.KEYS_NOT_EQUAL_VALUES,
							"MmeUeS1apId:" + this.mmeUeS1apId + ",smr size="
									+ this.smrs.size() + ",but v size="
									+ values.length);
				} else {
					for (int i = 0; i < this.smrs.size(); i++) {
						etlData.addData(this.smrs.get(i), values[i]);
					}
				}
			}

			this.etlData.addData(FIELD_STARTTIME, this.startTime);
			this.etlData.addData(FIELD_ENDTIME, this.endTime);
			this.etlData.addData(FIELD_TIMESTAMP, this.timeStamp);
			this.etlData.addData(FIELD_ENODEBID, this.eNodeID);
			this.etlData.addData(FIELD_CELLID, this.cellID);
			this.etlData.addData(FIELD_MMEGROUPID, this.mmeGroupId);
			this.etlData.addData(FIELD_MMEUES1APID, this.mmeUeS1apId);
			this.etlData.addData(FIELD_MMECODE, this.mmeCode);

			// when the source field is not contain the master key,throws
			// exception
			if (this.fieldIds != null && this.fieldIds.size() > 0) {
				List<String> fieldNames = this.etlData.getFieldNames();
				List<String> tempFieldIds = new ArrayList<String>();
				tempFieldIds.addAll(this.fieldIds);
				tempFieldIds.removeAll(fieldNames);
				if (tempFieldIds.size() > 0) {
					String errorMsg = tempFieldIds.toString();
					String errorCode = String.valueOf(errorMsg.hashCode());
					throw new ETLException(errorCode,
							"mro xml miss master key field:" + errorMsg
									+ ",current smr is:" + this.smrs.toString());
				}
			}
			return etlData;
		}

		return null;
	}

	private String getAttrValue(String str, String attrName)
			throws ETLException {
		if (attrName == null || attrName.trim().length() == 0) {
			return null;
		}

		String ret = null;
		try {
			int index = str.indexOf(attrName);
			if (index != -1) {
				int begin = index + attrName.length() + 2;
				String temp = str.substring(begin);
				int end = temp.indexOf("\"");
				ret = temp.substring(0, end);
			}
		} catch (Exception e) {
			throw new ETLException(ETLException.GET_ATTR_VALUE_ERROR,"get attribute value error,attr=" + str);
		}

		return ret;
	}

	private String getTagValue(String str, String tagName) throws ETLException {
		if (StringUtils.isBlank(str) || StringUtils.isBlank(tagName)) {
			return null;
		}

		String ret = null;
		try {
			int index = str.indexOf(tagName);
			if (index != -1) {
				int begin = index + tagName.length() + 1;
				int end = str.lastIndexOf("<");
				ret = str.substring(begin, end);
			}
		} catch (Exception e) {
			throw new ETLException(ETLException.GET_TAG_VALUE_ERROR,"get tag value error,tag=" + str);
		}

		return ret;
	}

}

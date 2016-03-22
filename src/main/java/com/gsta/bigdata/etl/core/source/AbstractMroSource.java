package com.gsta.bigdata.etl.core.source;

import java.util.ArrayList;
import java.util.Collections;
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
import com.gsta.bigdata.utils.SourceXmlTool;

/**
 * huawei mro xml source for gdnoce project
 *  demo data
<?xml version="1.0" encoding="UTF-8"?>
<bulkPmMrDataFile>
  <fileHeader fileFormatVersion="1.0" jobid="0" period="0" reportTime="2016-03-13T06:45:00.000" startTime="2016-03-13T06:30:00.000" endTime="2016-03-13T06:45:00.000"/>
  <eNB id="854406" userLabel="userLabel">
    <measurement>
      <smr>MR.LteScRSRP MR.LteNcRSRP MR.LteScRSRQ MR.LteNcRSRQ MR.LteScTadv MR.LteScPHR MR.LteScRIP MR.LteScPlrULQci1 MR.LteScPlrULQci2 MR.LteScPlrULQci3 MR.LteScPlrULQci4 MR.LteScPlrULQci5 MR.LteScPlrULQci6 MR.LteScPlrULQci7 MR.LteScPlrULQci8 MR.LteScPlrULQci9 MR.LteScPlrDLQci1 MR.LteScPlrDLQci2 MR.LteScPlrDLQci3 MR.LteScPlrDLQci4 MR.LteScPlrDLQci5 MR.LteScPlrDLQci6 MR.LteScPlrDLQci7 MR.LteScPlrDLQci8 MR.LteScPlrDLQci9 MR.LteScSinrUL MR.LteScEarfcn MR.LteScPci MR.LteScCgi MR.LteNcEarfcn MR.LteNcPci MR.GsmNcellBcch MR.GsmNcellCarrierRSSI MR.GsmNcellNcc MR.GsmNcellBcc MR.UtraCpichRSCP MR.UtraCpichEcNo MR.UtraCellParameterId MR.LteScAOA MR.LteScUeRxTxTD MR.LteSceEuRxTxTD MR.LteRSTD MR.LteTEuGNSS MR.LteTUeGNSS MR.LteFddNcRSRP MR.LteFddNcRSRQ MR.LteFddNcEarfcn MR.LteFddNcPci MR.LteTddNcRSRP MR.LteTddNcRSRQ MR.LteTddNcEarfcn MR.LteTddNcPci MR.UtraCarrierRSSI</smr>
      <object MmeCode="2" MmeGroupId="17408" MmeUeS1apId="50398167" TimeStamp="2016-03-13T06:30:03.520" id="854406-50">
        <v>40 29 26 15 NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL 1825 113 218727986 1825 111 NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL</v>
        <v>40 25 26 0 NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL 1825 113 218727986 1825 13 NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL</v>
        <v>40 20 26 4 NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL 1825 113 218727986 1825 207 NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL</v>
        <v>40 18 26 10 NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL 1825 113 218727986 1825 165 NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL</v>
        <v>40 14 26 0 NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL 1825 113 218727986 1825 189 NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL NIL</v>
      </object>
    </measurement>
  </eNB>
</bulkPmMrDataFile>   
 * 
 * @author tianxq
 *
 */
public class AbstractMroSource extends AbstractSourceMetaData {
	private static final long serialVersionUID = 1L;
	@JsonProperty
	private List<String> fieldIds = new ArrayList<String>();

	protected ETLData etlData = new ETLData();

	protected String startTime;
	protected String endTime;
	// the son of object "v"
	protected List<SMRObj> smrObjs = new ArrayList<SMRObj>();
	// object element
	protected MroObj mroObj = new MroObj();
	// smr fields
	protected List<String> smrs = new ArrayList<String>();

	protected static final String ATTR_STARTTIME = "startTime";
	protected static final String ATTR_ENDTIME = "endTime";
	protected static final String ATTR_TIMESTAMP = "TimeStamp";
	protected static final String ATTR_ID = "id";
	protected static final String ATTR_MMEGROUPID = "MmeGroupId";
	protected static final String ATTR_MMEUES1APID = "MmeUeS1apId";
	protected static final String ATTR_MMECODE = "MmeCode";

	protected static final String TAG_SMR = "smr";
	protected static final String TAG_V = "v";

	protected static final String FIELD_STARTTIME = "startTime";
	protected static final String FIELD_ENDTIME = "endTime";
	protected static final String FIELD_TIMESTAMP = "TimeStamp";
	protected static final String FIELD_ENODEBID = "ENODEID";
	protected static final String FIELD_CELLID = "CELLID";
	protected static final String FIELD_MMEGROUPID = "MmeGroupId";
	protected static final String FIELD_MMEUES1APID = "MmeUeS1apId";
	protected static final String FIELD_MMECODE = "MmeCode";

	public AbstractMroSource() {
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

	protected void computeHeader(String line) {
		if (line == null || "".equals(line)) {
			return;
		}

		if (line.indexOf("<fileHeader") != -1) {
			this.startTime = SourceXmlTool.getAttrValue(line, ATTR_STARTTIME).replace("T", " ");
			this.endTime = SourceXmlTool.getAttrValue(line, ATTR_ENDTIME).replace("T", " ");
		}
	}

	protected void computeSmrs(String line) {
		if (line == null || "".equals(line)) {
			return;
		}
		
		if (line.indexOf("<smr>") != -1) {
			this.smrs.clear();
			String smr = SourceXmlTool.getTagValue(line, TAG_SMR);
			if (StringUtils.isNotBlank(smr)) {
				String[] tempSmr = smr.replace(".", "_").split(" ");
				for (String temp : tempSmr) {
					this.smrs.add(temp);
				}
			}
		}
	}

	protected void computeObj(String line) {
		if (line == null || "".equals(line)) {
			return;
		}
		
		if (line.indexOf("<object") != -1) {
			this.etlData.clear();
			this.smrObjs.clear();
			this.mroObj.clear();

			String timeStamp = SourceXmlTool.getAttrValue(line, ATTR_TIMESTAMP).replace("T", " ");
			String id = SourceXmlTool.getAttrValue(line, ATTR_ID).trim();
			String mmeGroupId = SourceXmlTool.getAttrValue(line,ATTR_MMEGROUPID);
			String mmeUeS1apId = SourceXmlTool.getAttrValue(line,ATTR_MMEUES1APID);
			String mmeCode = SourceXmlTool.getAttrValue(line, ATTR_MMECODE);
			this.mroObj.setValues(id, mmeGroupId, mmeUeS1apId, mmeCode,timeStamp);
			this.mroObj.computeNodeAndCell();
		}
	}

	protected void computeV(String line) {
		if (line == null || "".equals(line)) {
			return;
		}
		
		if (line.indexOf("<v>") != -1) {
			String data = SourceXmlTool.getTagValue(line, TAG_V);
			if (StringUtils.isNotBlank(data)) {
				String[] values = data.split(" ");
				if (this.smrs.size() != values.length) {
					throw new ETLException(ETLException.KEYS_NOT_EQUAL_VALUES,
							"MmeUeS1apId:" + this.mroObj.getMmeUeS1apId()
							+ ",smr size=" + this.smrs.size() + ",but v size=" + values.length);
				}

				for (int i = 0; i < this.smrs.size(); i++) {
					// every SMR field cover the last one
					String key = this.smrs.get(i);
					this.etlData.addData(key, values[i]);
				}

				SMRObj smrObj = new SMRObj(
						this.etlData.getValue(SMRObj.FIELD_MR_LteNcRSRP),
						this.etlData.getValue(SMRObj.FIELD_MR_LteNcRSRQ),
						this.etlData.getValue(SMRObj.FIELD_MR_LteNcEarfcn),
						this.etlData.getValue(SMRObj.FIELD_MR_LteNcPci));
				this.smrObjs.add(smrObj);
			}// end if isNotBlank
		}
	}

	@SuppressWarnings("static-access")
	protected boolean emitData(String line) {
		if (line == null || "".equals(line)) {
			return false;
		}
		
		// emit data
		if (line.indexOf("</object>") != -1) {
			this.etlData.addData(FIELD_STARTTIME, this.startTime);
			this.etlData.addData(FIELD_ENDTIME, this.endTime);

			this.etlData.addData(FIELD_TIMESTAMP, this.mroObj.getTimeStamp());
			this.etlData.addData(FIELD_ENODEBID, this.mroObj.geteNodeID());
			this.etlData.addData(FIELD_CELLID, this.mroObj.getCellID());
			this.etlData.addData(FIELD_MMEGROUPID, this.mroObj.getMmeGroupId());
			this.etlData.addData(FIELD_MMEUES1APID,this.mroObj.getMmeUeS1apId());
			this.etlData.addData(FIELD_MMECODE, this.mroObj.getMmeCode());

			Collections.sort(this.smrObjs);
			for (int i = 0; i < this.smrObjs.size(); i++) {
				SMRObj smrObj = this.smrObjs.get(i);
				this.etlData.addData(smrObj.FIELD_MR_LteNcEarfcn + i,smrObj.getMR_LteNcEarfcn());
				this.etlData.addData(smrObj.FIELD_MR_LteNcPci + i,smrObj.getMR_LteNcPci());
				this.etlData.addData(smrObj.FIELD_MR_LteNcRSRP + i,smrObj.getMR_LteNcRSRP());
				this.etlData.addData(smrObj.FIELD_MR_LteNcRSRQ + i,smrObj.getMR_LteNcRSRQ());
			}
			
			return true;
		}
		
		return false;
	}

	@Override
	public ETLData parseLine(String line, Set<String> invalidRecords)
			throws ETLException, ValidatorException {
		this.computeHeader(line);
		this.computeSmrs(line);
		this.computeObj(line);
		this.computeV(line);
		if (this.emitData(line)) {
			this.verifyKeyField();
			return etlData;
		}
		
		return null;
	}

	/**
	 * when the source field is not contain the master key
	 * 
	 * @throws ETLException
	 */
	protected void verifyKeyField() throws ETLException {
		if (this.fieldIds != null && this.fieldIds.size() > 0) {
			List<String> fieldNames = this.etlData.getFieldNames();
			List<String> tempFieldIds = new ArrayList<String>();
			tempFieldIds.addAll(this.fieldIds);
			tempFieldIds.removeAll(fieldNames);
			if (tempFieldIds.size() > 0) {
				String errorMsg = tempFieldIds.toString();
				String errorCode = String.valueOf(errorMsg.hashCode());
				throw new ETLException(errorCode,
						"source miss master key field:" + errorMsg 
						+ ",current smr is:" + this.smrs.toString());
			}
		}
	}
}

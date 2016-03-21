package com.gsta.bigdata.etl.core.source;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.utils.SourceXmlTool;

/**
 * demo data
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
 * @author tianxq
 *
 */
public class MroHuaWei extends AbstractMroSource {
	private static final long serialVersionUID = 1L;
	private List<SMRObj> smrObjs=new ArrayList<SMRObj>();
	private MroObj mroObj = new MroObj();
	
	@SuppressWarnings("static-access")
	@Override
	public ETLData parseLine(String line, Set<String> invalidRecords)
			throws ETLException, ValidatorException {
		super.computeHeaderAndSmr(line);
		
		if (line.indexOf("<object") != -1) {
			super.etlData.clear();
			this.smrObjs.clear();
			this.mroObj.clear();

			String timeStamp = SourceXmlTool.getAttrValue(line, ATTR_TIMESTAMP).replace("T", " ");
			String id = SourceXmlTool.getAttrValue(line, ATTR_ID).trim();		
			String mmeGroupId = SourceXmlTool.getAttrValue(line, ATTR_MMEGROUPID);
			String mmeUeS1apId = SourceXmlTool.getAttrValue(line, ATTR_MMEUES1APID);
			String mmeCode = SourceXmlTool.getAttrValue(line, ATTR_MMECODE);
			this.mroObj.setValues(id, mmeGroupId, mmeUeS1apId, mmeCode, timeStamp);
			this.mroObj.computeNodeAndCell();
		}
		
		if (line.indexOf("<v>") != -1) {
			String data = SourceXmlTool.getTagValue(line, TAG_V);
			if (StringUtils.isNotBlank(data)) {
				String[] values = data.split(" ");
				if (super.smrs.size() != values.length) {
					throw new ETLException(ETLException.KEYS_NOT_EQUAL_VALUES,
							"MmeUeS1apId:" + this.mroObj.getMmeUeS1apId() + ",smr size="
							+ super.smrs.size() + ",but v size=" + values.length);
				}
				
				for (int i = 0; i < super.smrs.size(); i++) {
					//every SMR field cover the last one
					String key = super.smrs.get(i);
					super.etlData.addData(key, values[i]);
				}//end for
				
				SMRObj smrObj = new SMRObj(
						super.etlData.getValue(SMRObj.FIELD_MR_LteNcRSRP),
						super.etlData.getValue(SMRObj.FIELD_MR_LteNcRSRQ),
						super.etlData.getValue(SMRObj.FIELD_MR_LteNcEarfcn),
						super.etlData.getValue(SMRObj.FIELD_MR_LteNcPci));
				this.smrObjs.add(smrObj);
			}// end if isNotBlank
		}
		
		if(line.indexOf("</object>") != -1){			
			super.etlData.addData(FIELD_STARTTIME, this.startTime);
			super.etlData.addData(FIELD_ENDTIME, this.endTime);
			
			super.etlData.addData(FIELD_TIMESTAMP, this.mroObj.getTimeStamp());
			super.etlData.addData(FIELD_ENODEBID, this.mroObj.geteNodeID());
			super.etlData.addData(FIELD_CELLID, this.mroObj.getCellID());
			super.etlData.addData(FIELD_MMEGROUPID, this.mroObj.getMmeGroupId());
			super.etlData.addData(FIELD_MMEUES1APID, this.mroObj.getMmeUeS1apId());
			super.etlData.addData(FIELD_MMECODE, this.mroObj.getMmeCode());
			
			Collections.sort(this.smrObjs);
			for(int i=0;i<this.smrObjs.size();i++){
				SMRObj smrObj = this.smrObjs.get(i);
				super.etlData.addData(smrObj.FIELD_MR_LteNcEarfcn + i, smrObj.getMR_LteNcEarfcn());
				super.etlData.addData(smrObj.FIELD_MR_LteNcPci + i,smrObj.getMR_LteNcPci());
				super.etlData.addData(smrObj.FIELD_MR_LteNcRSRP + i,smrObj.getMR_LteNcRSRP());
				super.etlData.addData(smrObj.FIELD_MR_LteNcRSRQ + i,smrObj.getMR_LteNcRSRQ());
			}
			
			super.verifyKeyField();
			
			return etlData;
		}
		
		return null;
	}
}

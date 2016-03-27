package com.gsta.bigdata.etl.core.source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;

import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.utils.SourceXmlTool;

public class MroZte extends MroHuaWei {
	private static final long serialVersionUID = 1L;
	//the first and tow smr data,key=ZTEMroObj,value= objDatas index in list
	private Map<ZTEMroObj,Integer> firstSMRIdxs = new HashMap<ZTEMroObj,Integer>();
	//the third smr data,key =ZTEMroObj_1,value = objDatas index in list
	private Map<ZTEMroObj_1,Integer> thirdSMRIdxs = new HashMap<ZTEMroObj_1,Integer>();
	//etl data index
	private AtomicInteger etlDataIndex = new AtomicInteger(0);
	//save all object data of bulkPmMrDataFile
	private List<ETLData> objDatas = new ArrayList<ETLData>();
	private ZTEMroObj mroObj;
	private ZTEMroObj_1 mroObj_1;
	//key=object cig+timestamp,value=v of the same timestamp
	private Map<String,List<Long>> thirdObjDatas = new HashMap<String,List<Long>>();
	
	private String eNBId;
	private final static String ATTR_ENBID = "MR.eNBId";
	private final static String ATTR_MROBJ_ID = "MR.objectId";
	
	//1-smr include MR.LteScRSRP;2-include MR.LteScPlrULQci1;3-include MR.LteScRIP
	private int smrType = 0;
	
	public MroZte(){
		super();
	}
	
	public List<ETLData> parseLine(String line)
			throws ETLException, ValidatorException {
		if (line == null || "".equals(line)) {
			return null;
		}
		
		if(line.indexOf("<bulkPmMrDataFile>") != -1){
			super.startTime = null;
			super.endTime = null;
			this.smrType = 0;
			this.eNBId = null;
			this.thirdObjDatas.clear();
			this.firstSMRIdxs.clear();
			this.thirdSMRIdxs.clear();
			this.objDatas.clear();
			this.etlDataIndex = new AtomicInteger(0);
		}
		
		super.computeHeader(line);
		this.computeENBId(line);
		this.computeSmrsAndType(line);
		this.computeZTEObj(line);
		
		switch (this.smrType) {
		case 1:
			super.computeV(line);
			if(super.emitData(line,this.mroObj)){
				super.verifyKeyField();
				int idx = this.etlDataIndex.getAndIncrement();
				this.objDatas.add(idx, super.etlData);
				this.firstSMRIdxs.put(this.mroObj, idx);
				this.thirdSMRIdxs.put(this.mroObj_1, idx);
			}
			break;
		case 2:
			super.computeV(line);
			if(super.emitData(line,this.mroObj) &&
					this.firstSMRIdxs.containsKey(this.mroObj)){
				//merge one record
				Integer idx = this.firstSMRIdxs.get(this.mroObj);
				this.objDatas.get(idx.intValue()).addData(super.etlData);
			}
			break;
		case 3:
			//only one v line
			if(line.indexOf("<v>") != -1){
				String MRLteScRIP = SourceXmlTool.getTagValue(line, TAG_V);
				Long value = Long.parseLong(MRLteScRIP);
				String timeStamp = this.mroObj.getTimeStamp();
				String key = this.mroObj.getCgi() + "-" + timeStamp;
				if(this.thirdObjDatas.containsKey(key)){
					this.thirdObjDatas.get(key).add(value);
				}else{
					List<Long> values = new ArrayList<Long>();
					values.add(value);
					this.thirdObjDatas.put(key, values);
				}
			}
			//emit all third measurement data
			if(line.indexOf("</measurement>") != -1){
				for(Map.Entry<String, List<Long>> entry:this.thirdObjDatas.entrySet()){
					String key = entry.getKey();
					List<Long> values = entry.getValue();
					long sum = 0;
					for(Long v:values){
						sum += v.longValue();
					}
					long average = sum / values.size();
					ZTEMroObj_1 zteMroObj1 = new ZTEMroObj_1(key);
					if (this.thirdSMRIdxs.containsKey(zteMroObj1)) {
						Integer idx = this.thirdSMRIdxs.get(zteMroObj1);
						this.objDatas.get(idx.intValue()).addData(
								super.smrs.get(0), String.valueOf(average));
					}
				}//end for
			}
			break;
		}
		
		//emit all etldata
		if(line.indexOf("</bulkPmMrDataFile>") != -1){
			List<ETLData> ret = new ArrayList<ETLData>();
			for(ETLData etlData:this.objDatas){
				ret.add(etlData);
			}
			return ret;
		}
		
		return null;
	}
	
	private void computeENBId(String line){
		if (line == null || "".equals(line)) {
			return;
		}

		if (line.indexOf("<eNB") != -1) {
			this.eNBId = SourceXmlTool.getAttrValue(line, ATTR_ENBID);
		}
	}
	
	private void computeSmrsAndType(String line) {
		if (line == null || "".equals(line)) {
			return;
		}
		
		if (line.indexOf("<smr>") != -1) {
			super.smrs.clear();
			String smr = SourceXmlTool.getTagValue(line, TAG_SMR);
			if (StringUtils.isNotBlank(smr)) {
				String[] tempSmr = smr.replace(".", "_").split(" ");
				for (String temp : tempSmr) {
					super.smrs.add(temp);
				}
			}//end if
			
			if(smr.indexOf("MR.LteScRSRP") != -1){
				this.smrType = 1;
			}else if(smr.indexOf("MR.LteScPlrULQci1") != -1){
				this.smrType = 2;
			}else if(smr.indexOf("MR.LteScRIP") != -1){
				this.smrType = 3;
			}
		}
	}
	
	private void computeZTEObj(String line) {
		if (line == null || "".equals(line)) {
			return;
		}
		
		if (line.indexOf("<object") != -1) {
			super.etlData =  new ETLData();
			super.smrObjs.clear();
			this.mroObj = new ZTEMroObj();

			String timeStamp = SourceXmlTool.getAttrValue(line, ATTR_TIMESTAMP).replace("T", " ");
			String id = SourceXmlTool.getAttrValue(line, ATTR_ID);
			String mmeGroupId = SourceXmlTool.getAttrValue(line,ATTR_MMEGROUPID);
			String mmeUeS1apId = SourceXmlTool.getAttrValue(line,ATTR_MMEUES1APID);
			String mmeCode = SourceXmlTool.getAttrValue(line, ATTR_MMECODE);
			String mrObjId = SourceXmlTool.getAttrValue(line, ATTR_MROBJ_ID);
			this.mroObj.setValues(id, mmeGroupId, mmeUeS1apId, mmeCode,timeStamp,eNBId,mrObjId);
			this.mroObj.computeNodeAndCellAndCgi();
			
			this.mroObj_1 = new ZTEMroObj_1(this.mroObj);
		}
	}
}

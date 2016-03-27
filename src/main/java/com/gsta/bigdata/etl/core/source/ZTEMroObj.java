package com.gsta.bigdata.etl.core.source;

import org.apache.commons.lang3.StringUtils;

public class ZTEMroObj extends HuaweiMroObj{
	protected String cgi;
	protected String eNBId;
	protected String mrObjId;
	
	public void setValues(String id, String mmeGroupId, String mmeUeS1apId,
			String mmeCode, String timeStamp,String eNBId,String mrObjId) {
		super.setValues(id, mmeGroupId, mmeUeS1apId, mmeCode, timeStamp);
		this.eNBId = eNBId;
		this.mrObjId = mrObjId;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj == null){
			return false;
		}
		
		if(this == obj){
			return true;
		}
		
		if(obj.getClass() == ZTEMroObj.class){
			ZTEMroObj mroObj = (ZTEMroObj)obj;
			return mroObj.getId().equals(this.id) &&
				   mroObj.getMmeUeS1apId().equals(this.mmeUeS1apId) &&
				   mroObj.getMmeCode().equals(this.mmeCode) &&
				   mroObj.getTimeStamp().equals(this.timeStamp);
		}
		
		return false;
	}
	
	public void computeNodeAndCellAndCgi(){
		if(StringUtils.isNotBlank(super.id)){
			String temp;
			if(super.id.indexOf(":") != -1){
				temp = super.id.substring(0, super.id.indexOf(":"));
			}else{
				temp = super.id;
			}
			if(temp != null){
				super.eNodeID = String.valueOf(Integer.parseInt(temp) / 256);
				super.cellID = String.valueOf(Integer.parseInt(temp) % 256);
				this.cgi = temp;
			}
		}else{
			super.eNodeID = this.eNBId;
			super.cellID = this.mrObjId;
			this.cgi = String.valueOf(Integer.parseInt(super.eNodeID) * 256
					+ Integer.parseInt(this.mrObjId));
		}
	}
	
	public String getCgi() {
		return cgi;
	}
}

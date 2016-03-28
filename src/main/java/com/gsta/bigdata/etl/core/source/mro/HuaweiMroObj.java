package com.gsta.bigdata.etl.core.source.mro;

import org.apache.commons.lang.StringUtils;

public class HuaweiMroObj {
	protected String id;
	protected String eNodeID;
	protected String cellID;
	protected String mmeGroupId;
	protected String mmeUeS1apId;
	protected String mmeCode;
	protected String timeStamp;
	
	public void setValues(String id,String mmeGroupId,String mmeUeS1apId,String mmeCode,String timeStamp){
		this.id = id;
		this.mmeGroupId = mmeGroupId;
		this.mmeUeS1apId = mmeUeS1apId;
		this.mmeCode = mmeCode;
		this.timeStamp = timeStamp;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj == null){
			return false;
		}
		
		if(this == obj){
			return true;
		}
		
		if(obj.getClass() == HuaweiMroObj.class){
			HuaweiMroObj mroObj = (HuaweiMroObj)obj;
			return mroObj.getId().equals(this.id) &&
				   mroObj.getMmeUeS1apId().equals(this.mmeUeS1apId) &&
				   mroObj.getMmeCode().equals(this.mmeCode) &&
				   mroObj.getTimeStamp().equals(this.timeStamp);
		}
		
		return false;
	}
	
	public void computeNodeAndCell(){
		if(this.id == null || "".equals(this.id)){
			return;
		}
		
		if (StringUtils.isNotBlank(this.id)) {
			String[] ids = this.id.split("-");
			// mro-hw object id
			if (ids.length == 2) {
				this.eNodeID = ids[0];
				this.cellID = ids[1];
			}
		}
	}
	
	public String getId() {
		return id;
	}
	
	public String geteNodeID() {
		return eNodeID;
	}
	
	public String getCellID() {
		return cellID;
	}
	
	public String getMmeGroupId() {
		return mmeGroupId;
	}
	
	public String getMmeUeS1apId() {
		return mmeUeS1apId;
	}
	
	public String getMmeCode() {
		return mmeCode;
	}
	
	public String getTimeStamp() {
		return timeStamp;
	}
}

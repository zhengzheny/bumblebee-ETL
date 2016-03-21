package com.gsta.bigdata.etl.core.source;

import org.apache.commons.lang.StringUtils;

import com.gsta.bigdata.etl.ETLException;

public class MroObj {
	private String id;
	private String eNodeID;
	private String cellID;
	private String mmeGroupId;
	private String mmeUeS1apId;
	private String mmeCode;
	private String timeStamp;
	
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
		
		if(obj.getClass() == MroObj.class){
			MroObj mroObj = (MroObj)obj;
			return mroObj.getId().equals(this.id) &&
				   mroObj.getMmeUeS1apId().equals(this.mmeUeS1apId) &&
				   mroObj.getMmeCode().equals(this.mmeCode) &&
				   mroObj.getTimeStamp().equals(this.timeStamp);
		}
		
		return false;
	}
	
	public void clear(){
		this.cellID = null;
		this.eNodeID = null;
		this.id = null;
		this.mmeCode = null;
		this.mmeGroupId = null;
		this.mmeUeS1apId = null;
		this.timeStamp = null;
	}
	
	public void computeNodeAndCell(){
		if(this.id == null || "".equals(this.id)){
			return;
		}
		
		if (StringUtils.isNotBlank(this.id)) {
			String[] ids = this.id.split("-");
			// mro-zte object id
			if (ids.length == 1) {
				try {
					this.eNodeID = String.valueOf(Integer.parseInt(this.id) / 256);
					this.cellID = String.valueOf(Integer.parseInt(this.id) % 256);
				} catch (NumberFormatException e) {
					throw new ETLException(ETLException.MRO_XML_ID_ERROR,"mro xml id is error,id=" + id);
				}
			}
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

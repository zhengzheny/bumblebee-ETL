package com.gsta.bigdata.etl.core.source.mro;


public class ZTEMroObj extends HuaweiMroObj{
	protected String cgi;
	protected String eNBId;
	protected String mrObjId;
	
	public void setValues(String id, String mmeGroupId, String mmeUeS1apId,
			String mmeCode, String timeStamp,String eNBId,String mrObjId) {
		super.setValues(id, mmeGroupId, mmeUeS1apId, mmeCode, timeStamp);
		this.eNBId = eNBId;
		this.mrObjId = mrObjId;
		if(super.id == null){
			super.id = this.mrObjId;
		}
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
	
	@Override
	public int hashCode() {
		int ret = 0;
		if(this.id != null){
			ret += this.id.hashCode();
		}
		if(this.mmeUeS1apId != null){
			ret += this.mmeUeS1apId.hashCode();
		}
		if(this.mmeCode != null){
			ret += this.mmeCode.hashCode();
		}
		if(this.timeStamp != null){
			ret += this.timeStamp.hashCode();
		}
		return ret;
	}

	public void computeNodeAndCellAndCgi() {
		int id = this.getId(super.id);
		if (id > 0) {
			super.eNodeID = String.valueOf(id / 256);
			super.cellID = String.valueOf(id % 256);
			this.cgi = String.valueOf(id);
		} else {
			super.eNodeID = this.eNBId;
			super.cellID = this.mrObjId;
			this.cgi = String.valueOf(Integer.parseInt(super.eNodeID) * 256
					+ this.getId(this.mrObjId));
		}
	}
	
	private int getId(String id){
		if(id == null || "".equals(id)){
			return -1;
		}
		
		String temp ;
		if(id.indexOf(":") != -1){
			temp = id.substring(0, id.indexOf(":"));
		}else{
			temp = id;
		}
		
		if(temp != null){
			return Integer.parseInt(temp);
		}else{
			return -1;
		}
	}
	
	public String getCgi() {
		return cgi;
	}
}

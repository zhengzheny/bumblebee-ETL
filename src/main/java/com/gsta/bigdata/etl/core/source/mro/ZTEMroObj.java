package com.gsta.bigdata.etl.core.source.mro;

public class ZTEMroObj extends HuaweiMroObj{
	protected String cgi;
	protected String eNBId;
	protected String mrObjId;
	protected String eNBIdName;
	//if id is null,instead of mrObjId
	private String compareId;
	//private Logger logger = LoggerFactory.getLogger(getClass());
	
	public void setValues(String id, String mmeGroupId, String mmeUeS1apId,
			String mmeCode, String timeStamp,String eNBId,String mrObjId,String eNBIdName) {
		super.setValues(id, mmeGroupId, mmeUeS1apId, mmeCode, timeStamp);
		this.eNBId = eNBId;
		this.mrObjId = mrObjId;
		
		this.compareId = super.id;
		if(this.compareId == null){
			this.compareId = this.mrObjId;
		}
		this.eNBIdName = eNBIdName;
	}
	
	@Override
	public String toString() {
		return "cgi=" + cgi + ",eNBId=" + eNBId + ",mrObjId=" + mrObjId
				+ ",eNBIdName=" + eNBIdName + ",compareId=" + compareId;
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
			return mroObj.getCompareId().equals(this.compareId) &&
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
		int id = this.parseId(super.id);
		if (id > 0) {
			super.eNodeID = String.valueOf(id / 256);
			super.cellID = String.valueOf(id % 256);
			this.cgi = String.valueOf(id);
		} else {
			super.eNodeID = this.eNBId;
			super.cellID = this.mrObjId;
			try {
				this.cgi = String.valueOf(Integer.parseInt(super.eNodeID) * 256
						+ this.parseId(this.mrObjId));
			} catch (NumberFormatException e) {
				this.cgi = null;
			}
		}
	}
	
	private int parseId(String id){
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

	public String geteNBId() {
		return eNBId;
	}

	public String getMrObjId() {
		return mrObjId;
	}

	public String geteNBIdName() {
		return eNBIdName;
	}

	public String getCompareId() {
		return compareId;
	}
}

package com.gsta.bigdata.etl.core.source;

public class ZTEMroObj_1 extends ZTEMroObj {
	public ZTEMroObj_1(ZTEMroObj zteMroObj) {
		super.cgi = zteMroObj.getCgi();
		super.timeStamp = zteMroObj.getTimeStamp();
	}
	
	public ZTEMroObj_1(String key){
		if(key != null){
			String[] ids = this.id.split("-");
			if(ids.length == 2){
				super.cgi = ids[0];
				super.timeStamp = ids[1];
			}
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
		
		if(obj.getClass() == ZTEMroObj_1.class){
			ZTEMroObj_1 mroObj = (ZTEMroObj_1)obj;
			return mroObj.getCgi().equals(super.cgi) &&
				   mroObj.getTimeStamp().equals(super.timeStamp);
		}
		
		return false;
	}
}

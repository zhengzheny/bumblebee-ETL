package com.gsta.bigdata.etl.core.source.mro;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DIMObj implements java.io.Serializable{
	private static final long serialVersionUID = -1446921210109906173L;
	//key=eNodeB_ID-CELL_ID
	public final static String KEY_FIELD_DELIMITER = "-";
	public final static String NC_DELIMITER = ";";
	
	@JsonProperty
	private String city;
	@JsonProperty
	private String eNodeB_ID;
	@JsonProperty
	private String CELL_ID;
	@JsonProperty
	private String high;
	@JsonProperty
	private String Cell_power;
	@JsonProperty
	private String longitude;
	@JsonProperty
	private String latitude;
	@JsonProperty
	private String ANT_gain;
	@JsonProperty
	private String ANT_azimuth;
	@JsonProperty
	private String down_band;
	@JsonProperty
	private String band_width;
	@JsonProperty
	private String[] nc_CELL_ID;
	@JsonProperty
	private String[] nc_eNodeB_ID;
	
	public DIMObj(){
		
	}
	
	public DIMObj(String[] datas){
		if(datas != null && datas.length >= 12){
			this.city = datas[0];
			this.eNodeB_ID = datas[1];
			this.CELL_ID = datas[2];
			this.high = datas[3];
			this.Cell_power = datas[4];
			this.longitude = datas[5];
			this.latitude = datas[6];
			this.ANT_gain = datas[7];
			this.ANT_azimuth = datas[8];
			this.down_band = datas[9];
			this.band_width = datas[10];
			this.nc_eNodeB_ID = datas[11].split(NC_DELIMITER,-1);
			this.nc_CELL_ID = datas[12].split(NC_DELIMITER,-1);
		}
	}
	
	//only for sizeof test
	public String getKey(){
		return this.eNodeB_ID + KEY_FIELD_DELIMITER + this.CELL_ID;
	}
	
	/**
	 * get eNodeB_ID and cell_ID
	 * @param pciIdx
	 * @return
	 */
	public String[] getkeyByPCIIdx(int pciIdx) {
		if (pciIdx < 0) {
			return null;
		}

		if (pciIdx < this.nc_eNodeB_ID.length && pciIdx < this.nc_CELL_ID.length) {
			String[] ret = new String[2];
			ret[0] = this.nc_eNodeB_ID[pciIdx];
			ret[1] = this.nc_CELL_ID[pciIdx];
			return ret;
		}

		return null;
	}
	
	/**
	 * according MR_LteNcPci1 or MR_LteNcPci2,get nc_* values
	 * @param fields - output id from function
	 * @return
	 */
	public Map<String, String> getNCData(List<String> fields){
		if(fields == null || fields.size() < 11){
			return null;
		}
		
		Map<String, String> ret = new HashMap<String, String>();
		
		ret.put(fields.get(0), this.city);
		ret.put(fields.get(1), this.eNodeB_ID);
		ret.put(fields.get(2), this.CELL_ID);
		ret.put(fields.get(3), this.high);
		ret.put(fields.get(4), this.Cell_power);
		ret.put(fields.get(5), this.longitude);
		ret.put(fields.get(6), this.latitude);
		ret.put(fields.get(7), this.ANT_gain);
		ret.put(fields.get(8), this.ANT_azimuth);
		ret.put(fields.get(9), this.down_band);
		ret.put(fields.get(10), this.band_width);
		
		return ret;
	}
	
	/**
	 * get DIM basic info
	 * @param fields - output id from function
	 * @return
	 */
	public Map<String,String> getBasicInfo(List<String> fields){
		if(fields == null || fields.size() < 9){
			return null;
		}
		
		Map<String, String> ret = new HashMap<String, String>();
		ret.put(fields.get(0), this.city);
		ret.put(fields.get(1), this.high);
		ret.put(fields.get(2), this.Cell_power);
		ret.put(fields.get(3), this.longitude);
		ret.put(fields.get(4), this.latitude);
		ret.put(fields.get(5), this.ANT_gain);
		ret.put(fields.get(6), this.ANT_azimuth);
		ret.put(fields.get(7), this.down_band);
		ret.put(fields.get(8), this.band_width);
		
		return ret;
	}
}

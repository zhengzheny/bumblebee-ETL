package com.gsta.bigdata.etl.core.source.mro;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DIMObj {
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
	private String nc_city;
	@JsonProperty
	private String nc_eNodeB_ID;
	@JsonProperty
	private String nc_CELL_ID;
	@JsonProperty
	private String nc_high;
	@JsonProperty
	private String nc_Cell_power;
	@JsonProperty
	private String nc_longitude;
	@JsonProperty
	private String nc_latitude;
	@JsonProperty
	private String nc_ANT_gain;
	@JsonProperty
	private String nc_ANT_azimuth;
	@JsonProperty
	private String nc_down_band;
	@JsonProperty
	private String nc_band_width;
	
	public DIMObj(String[] datas){
		if(datas != null && datas.length >= 22){
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
			this.nc_city = datas[11];
			this.nc_eNodeB_ID = datas[12];
			this.nc_CELL_ID = datas[13];
			this.nc_high = datas[14];
			this.nc_Cell_power = datas[15];
			this.nc_longitude = datas[16];
			this.nc_latitude = datas[17];
			this.nc_ANT_gain = datas[18];
			this.nc_ANT_azimuth = datas[19];
			this.nc_down_band = datas[20];
			this.nc_band_width = datas[21];
		}
	}
	
	public String[] getPartData(int index,int type){
		//TODO::
		return null;
	}
}

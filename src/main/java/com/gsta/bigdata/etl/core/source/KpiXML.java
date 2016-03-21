package com.gsta.bigdata.etl.core.source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.StringUtils;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.Field;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.utils.SourceXmlTool;

/**
 * parse kpi xml by String
 * 
 * @author shine
 *
 */
public class KpiXML extends AbstractSourceMetaData {
	private static final long serialVersionUID = 4972621315058493751L;

	private String beginMt = "<mt>";
	private String beginMts = "<mts>";
	private String beginMi = "<mi>";
	private String beignMoid = "<moid>";
	private String beginGp = "<gp>";
	private String beginR = "<r>";
	private String endMv = "</mv>";

	private String statTime;
	private String statPeriod;
	private String moid;
	//mts counter
	private AtomicLong mtsCounter = new AtomicLong();

	private ETLData etlData = new ETLData();
	
	@JsonProperty
	private Map<String, String> sourceFields = new HashMap<String, String>();
	@JsonProperty
	private List<String> masterKeyFieldList = new ArrayList<String>();

	private List<String> mts = new ArrayList<String>();
	private List<String> rs = new ArrayList<String>();
	
	// record moid
	private Map<String, String> moidMap = new HashMap<String, String>();

	private static final String TAG_MTS = "mts";
	private static final String TAG_GP = "gp";
	private static final String TAG_MT = "mt";
	private static final String TAG_MOID = "moid";
	private static final String TAG_R = "r";

	private static final String FIELD_STAT_TIME = "STAT_TIME";
	private static final String FIELD_STAT_PERIOD = "STAT_PERIOD";
	
	public KpiXML() {
		super();
	}
	
	@Override
	protected void createChildNodeList(NodeList nodeList) throws ParseException {
		super.createChildNodeList(nodeList);
		
		Iterator<Field> iter = super.getFields().iterator();
		while (iter.hasNext()) {
			Field field = iter.next();
			this.sourceFields.put(field.getDesc(), field.getId());
			if(field.isMasterKey()){
				this.masterKeyFieldList.add(field.getDesc());
			}
		}
	}

	@Override
	public ETLData parseLine(String line, Set<String> invalidRecords)
			throws ETLException, ValidatorException {
			if (line.contains(beginMi)) {
				this.etlData.clear();
				this.mts.clear();
			}

			if (line.indexOf(beginMts) != -1) {
				this.mtsCounter.getAndIncrement();
				
				this.statTime = SourceXmlTool.getTagValue(line, TAG_MTS);
			}

			if (line.indexOf(beginGp) != -1) {
				this.statPeriod = SourceXmlTool.getTagValue(line, TAG_GP);
				if (StringUtils.isNotBlank(this.statPeriod)) {
					this.statPeriod = String.valueOf(Integer
							.parseInt(this.statPeriod) * 60);
				}
			}

			if (line.indexOf(beginMt) != -1) {
				String mt = SourceXmlTool.getTagValue(line, TAG_MT).trim();
				this.mts.add(mt);
			}

			if (line.indexOf(beignMoid) != -1) {
				this.moidMap.clear();
				this.rs.clear();

				this.moid = SourceXmlTool.getTagValue(line, TAG_MOID);
				this.splitMoid(this.moid);
			}

			if (line.indexOf(beginR) != -1) {
				String r = SourceXmlTool.getTagValue(line, TAG_R);
				this.rs.add(r);
			}
			
			//from the second mts begin output value
			if (line.indexOf(endMv) != -1 && this.mtsCounter.get() >= 2) {
				if (this.mts.size() != this.rs.size()) {
					throw new ETLException(ETLException.KEYS_NOT_EQUAL_VALUES,"moid:" + this.moid + ",mt size="
							+ this.mts.size() + ",but r size=" + this.rs.size());
				}

				this.etlData.addData(FIELD_STAT_TIME, this.statTime);
				this.etlData.addData(FIELD_STAT_PERIOD, this.statPeriod);
				
				//set ENODEB_NMAE,LOCACELLID,CELLID_NAME,ENODEB_ID,CELDUPLEXMODE
				for (Entry<String, String> entry : this.moidMap.entrySet()) {
					String key = sourceFields.get(entry.getKey());
					this.etlData.addData(key, entry.getValue());
				}

				for (int i = 0; i < this.mts.size(); i++) {
					String key = this.mts.get(i);
					this.etlData.addData(sourceFields.get(key), this.rs.get(i));
				}
				
				//when the source field is not contain the master key,throws exception
			if (this.masterKeyFieldList.size() > 0) {
				List<String> tempFieldIds = new ArrayList<String>();
				tempFieldIds.addAll(this.masterKeyFieldList);
				tempFieldIds.removeAll(this.mts);
				if (tempFieldIds.size() > 0) {
					String errorMsg = tempFieldIds.toString();
					String errorCode = String.valueOf(errorMsg.hashCode());
					throw new ETLException(errorCode,
							"kpi xml miss master key field:" + errorMsg + ",current mts is:" + this.mts.toString());
				}
			}

				return etlData;
			}

		return null;
	}

	private void splitMoid(String str) {
		if (StringUtils.isBlank(str)) {
			return;
		}

		String temp = str.substring(str.indexOf(":") + 1);

		String[] split = temp.split(",");
		if (split != null && split.length > 0) {
			for (String s : split) {
				String[] value = s.trim().split("=");
				if(value.length >= 2){
					this.moidMap.put(value[0], value[1]);
				}
			}
		}
	}
}

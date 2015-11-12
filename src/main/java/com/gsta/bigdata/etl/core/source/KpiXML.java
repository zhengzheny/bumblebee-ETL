package com.gsta.bigdata.etl.core.source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.Field;
import com.gsta.bigdata.etl.core.ParseException;

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

	private ETLData etlData = new ETLData();
	
	@JsonProperty
	private Map<String, String> sourceFields = new HashMap<String, String>();

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
		}
	}

	@Override
	public ETLData parseLine(String line, Set<String> invalidRecords)
			throws ETLException, ValidatorException {
		try {
			if (line.contains(beginMi)) {
				this.etlData.clear();
				this.mts.clear();
			}

			if (line.indexOf(beginMts) != -1) {
				this.statTime = this.getTagValue(line, TAG_MTS);
			}

			if (line.indexOf(beginGp) != -1) {
				this.statPeriod = this.getTagValue(line, TAG_GP);
				if (StringUtils.isNotBlank(this.statPeriod)) {
					this.statPeriod = String.valueOf(Integer
							.parseInt(this.statPeriod) * 60);
				}
			}

			if (line.indexOf(beginMt) != -1) {
				String mt = this.getTagValue(line, TAG_MT).trim();
				this.mts.add(mt);
			}

			if (line.indexOf(beignMoid) != -1) {
				this.moidMap.clear();
				this.rs.clear();

				this.moid = this.getTagValue(line, TAG_MOID);
				this.splitMoid(this.moid);
			}

			if (line.indexOf(beginR) != -1) {
				String r = this.getTagValue(line, TAG_R);
				this.rs.add(r);
			}

			if (line.indexOf(endMv) != -1) {
				if (this.mts.size() != this.rs.size()) {
					throw new ETLException("moid:" + this.moid + ",mt size="
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

				return etlData;
			}

		} catch (Exception e) {
			throw new ETLException(ETLException.KPI_XML_ERROR, "KpiXml error:"
					+ e.toString());
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
				this.moidMap.put(value[0], value[1]);
			}
		}
	}

	private String getTagValue(String str, String tagName) {
		if (StringUtils.isBlank(str) || StringUtils.isBlank(tagName)) {
			return null;
		}

		String ret = null;
		int index = str.indexOf(tagName);
		if (index != -1) {
			int begin = index + tagName.length() + 1;
			int end = str.lastIndexOf("<");
			ret = str.substring(begin, end);
		}

		return ret;
	}

}

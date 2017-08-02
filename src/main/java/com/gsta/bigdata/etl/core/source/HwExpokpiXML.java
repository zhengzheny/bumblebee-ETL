package com.gsta.bigdata.etl.core.source;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.Field;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.utils.SourceXmlTool;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.NodeList;

import java.util.*;

/**
 * parse zte xml by String
 * 
 * @author shine
 *
 */
public class HwExpokpiXML extends AbstractSourceMetaData {
	private static final long serialVersionUID = 4972621315058493751L;

	private static final String beginMeasData = "<measData>";
    private static final String endMeasData = "</measData>";
    private static final String beginmeasInfo = "<measInfo";
    private static final String endmeasInfo = "</measInfo";
	private static final String beginMeasCollec = "<measCollec beginTime";
	private static final String repPeriod = "<repPeriod";
    private static final String measValue = "<measValue";


	private String beginTime;
	private String stat_period;
    private String cgi;
    private String measInfoId ;
	private ETLData etlData = new ETLData();
	private Map<String,ETLData> mapEtl = new HashMap<>();
	private String[] types;
    private ArrayList<String>  strArray = new ArrayList<String> ();
    private boolean flag=false;
    private boolean eNodeBFlag=false;
	@JsonProperty
	private List<String> fieldIds = new ArrayList<String>();

	// date format pattern
	private String oldPattern = "yyyy-MM-dd'T'HH:mm:ss";
	private String newPattern = "yyyyMMddHHmmss";

	private static final String ATTR_BEGINTIME = "beginTime";
	private static final String ATTR_MEASOBJLDN = "measObjLdn";
	private static final String ATTR_DURATION = "duration";

	private static final String TAG_MEASTYPES = "measTypes";
	private static final String TAG_MEASRESULTS = "measResults";

	private static final String FIELD_ENODEBNAME = "eNodeB名称";
	private static final String FIELD_CELLNAME = "小区名称";
	private static final String FIELD_ENODEBID = "eNodeB标识";
	private static final String FIELD_CELLID = "小区标识";
	private static final String FIELD_STAT_PERIOD = "STAT_PERIOD";

    private static final List<String> measInfoIds= new ArrayList<String>(Arrays.asList ("1526726702","1526726694",
            "1526726666", "1526726657",
            "1526726683", "1526726684",
            "1526726729", "1526726659",
            "1526726661", "1526726660",
            "1526726700", "1526726698",
            "1526726664", "1526726722",
            "1526726705", "1526726708",
            "1526726710","1526726695"));
	private Logger logger = LoggerFactory.getLogger(getClass());

	public HwExpokpiXML() {
		super();
	}

	@Override
	protected void createChildNodeList(NodeList nodeList) throws ParseException {
		super.createChildNodeList(nodeList);

		// save verify field
		Iterator<Field> iter = super.getFields().iterator();
		while (iter.hasNext()) {
			Field field = iter.next();

			this.fieldIds.add(field.getId());
		}
	}

	@Override
	public ETLData parseLine(String line, Set<String> invalidRecords)
			throws ETLException, ValidatorException {
//            TODO
		return null;
	}

	private  ArrayList<String> splitMeasObjLdn(String measObjLdn) {
		if (StringUtils.isBlank(measObjLdn)) {
			return null;
		}
		String ecgi = "";
        ArrayList<String>  MeasObjLdns = new ArrayList<String> ();
        String[] objLdns = measObjLdn.split(", ");
		for (String objLdn : objLdns) {
			String[] obj = objLdn.split("=");
//			logger.info("objs:"+obj[0]);
			if (FIELD_ENODEBID .equals(obj[0])) {
//                this.etlData.addData("ENODEBID", obj[1]);
                MeasObjLdns.add(obj[1]);
				ecgi = obj[1];
			} else if (FIELD_CELLID .equals(obj[0])) {
//                this.etlData.addData("CELLID", obj[1]);
                MeasObjLdns.add(obj[1]);
				ecgi = ecgi +obj[1];
			} else if (obj[0].contains(FIELD_ENODEBNAME)) {
//                this.etlData.addData("ENODEBNAME", obj[1]);
                MeasObjLdns.add(obj[1]);
//                System.out.println("eNodeB名称"+obj[1]);
			} else if (FIELD_CELLNAME.equals(obj[0])) {
//                this.etlData.addData("CELLNAME", obj[1]);
                MeasObjLdns.add(obj[1]);
			}
		}
        MeasObjLdns.add(ecgi);
		return MeasObjLdns;
	}

	@Override
	public List<ETLData> parseLine(String line) throws ETLException,
            ValidatorException {
        if (line.contains(beginMeasData)) {
//            etlData.clear();
            mapEtl.clear();
        }
//		STAT_TIME
        if (line.trim().startsWith(beginMeasCollec)) {
//		    logger.info("time line:"+line);
            this.beginTime = SourceXmlTool.getAttrValue(line, ATTR_BEGINTIME);
//			logger.info("begintime:"+this.beginTime );
            if (StringUtils.isNotBlank(this.beginTime)) {
                try {
                    this.beginTime = com.gsta.bigdata.utils.StringUtils
                            .dateFormat(this.beginTime, oldPattern, newPattern);
                } catch (java.text.ParseException e) {
                    logger.error("beginTime:" + this.beginTime
                            + " format error");
                }
            }
            this.beginTime = this.beginTime.substring(0,this.beginTime.length()- 2);

        }
        if(line.trim().startsWith(beginmeasInfo)){
            measInfoId= SourceXmlTool.getAttrValue(line, "measInfoId");
            if (measInfoIds.contains(measInfoId)){
                flag=true;
                if(measInfoId.equals("1526726695")){
                    eNodeBFlag=true;
//                    logger.info("eNodeBFlag:true");
                }
            }
        }
        if(line.trim().startsWith(endmeasInfo)){
            flag=false;
            eNodeBFlag=false;
        }

        if (flag) {
//		STAT_PERIOD
            if (line.trim().startsWith(repPeriod)) {
                stat_period = SourceXmlTool.getAttrValue(line, ATTR_DURATION);
                int period = Integer.valueOf(stat_period.substring(2, 6)) / 3600;
                stat_period = String.valueOf(period) + "h";
//                this.etlData.addData(FIELD_STAT_PERIOD, stat_period);
//                this.etlData.addData("STAT_TIME", this.beginTime);
            }
//		cgi
            String measObjLdn = "";
            if (line.trim().startsWith(measValue)){
                if(!eNodeBFlag){
//                    logger.info("eNodeBFlag+"+eNodeBFlag);
                    measObjLdn = SourceXmlTool.getAttrValue(line, ATTR_MEASOBJLDN);
                    strArray = this.splitMeasObjLdn(measObjLdn);
                    cgi = strArray.get(4);
                    if (cgi == null) {
                        throw new ETLException(line + " has null cgi:" + measObjLdn);
                    } else if (!mapEtl.containsKey(cgi)) {
                        ETLData etlData = new ETLData();
                        etlData.addData(FIELD_STAT_PERIOD, this.stat_period);
                        etlData.addData("STAT_TIME", this.beginTime);
                        etlData.addData("ENODEBNAME", strArray.get(0));
                        etlData.addData("CELLNAME", strArray.get(1));
                        etlData.addData("ENODEBID", strArray.get(2));
                        etlData.addData("CELLID", strArray.get(3));
                        mapEtl.put(cgi, etlData);
//                        logger.info("new cgi:" + cgi);
                    }
            }}

            String measTypes = SourceXmlTool.getTagValue(line, TAG_MEASTYPES);
            if (StringUtils.isNotBlank(measTypes)) {
                this.types = measTypes.split(" ");
                if (this.types == null || this.types.length == 0) {
                    throw new ETLException(line + " has null " + TAG_MEASTYPES);
                }
            }

            String measResults = SourceXmlTool.getTagValue(line, TAG_MEASRESULTS);
            if (StringUtils.isNotBlank(measResults)) {
                String[] values = measResults.split(" ");
                if (values == null || values.length == 0) {
                    throw new ETLException(line + " has null " + TAG_MEASRESULTS);
                }
                if (this.types.length != values.length) {
                    throw new ETLException("measObjLdn:" + measObjLdn
                            + ",measTypes size=" + this.types.length
                            + ",but measResults size=" + values.length);
                } else {
                    if(eNodeBFlag){
                        for (ETLData v : mapEtl.values()) {
                            for (int i = 0; i < this.types.length; i++) {
                                v.addData(types[i], values[i]);
                            }
                        }
                    }else {
                        for (int i = 0; i < this.types.length; i++) {
                            mapEtl.get(cgi).addData(types[i], values[i]);
                        }
                    }
                }
            }
        }
        if (line.contains(endMeasData)) {
            if (this.fieldIds != null && this.fieldIds.size() > 0) {
                for (String fieldName : etlData.getFieldNames()) {
                    // check data field
                    if (this.fieldIds.contains(fieldName)) {
                        Field field = super.getFieldById(fieldName);
                        String fieldValue = etlData.getData().get(fieldName);
                        super.fieldValidate(field, fieldValue, fieldValue,
                                null);
                    }
                }
            }
            List<ETLData> DATALIST = new ArrayList<>();
            for (Map.Entry<String, ETLData> entry : mapEtl.entrySet()){
                DATALIST.add( entry.getValue());
            }
                return DATALIST;
        }

        return null;
	}


}

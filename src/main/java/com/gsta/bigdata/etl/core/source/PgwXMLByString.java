package com.gsta.bigdata.etl.core.source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.Field;
import com.gsta.bigdata.etl.core.ParseException;

/**
 * parse pgwXml by string
 * @author shine
 *
 */

public class PgwXMLByString extends AbstractSourceMetaData {
	private static final long serialVersionUID = -6542179672837906742L;
	private List<String> xmlList = new ArrayList<String>();
	private boolean xmlFlag = false;
	private boolean listServiceFlag = false;

	@JsonProperty
	private List<String> fieldIds = new ArrayList<String>();

	private String beginContent = "<Content>";
	private String endContent = "</Content>";
	private String benginListService = "<listService>";
	private String endListService = "</listService>";

	private static final String TAG_RECTYPE = "recType";
	private static final String TAG_IMSI = "IMSI";
	private static final String TAG_PGWADDR = "pGWAddr";
	private static final String TAG_CHARGID = "chargID";
	private static final String TAG_NODEADDR = "NodeAddr";
	private static final String TAG_APN = "APN";
	private static final String TAG_PDPPDNTYPE = "pdpPDNType";
	private static final String TAG_PDPPDNADDR = "PDPPDNAddr";
	private static final String TAG_DADDRFLAG = "dAddrFlag";
	private static final String TAG_UPLINK = "Uplink";
	private static final String TAG_DOWNLINK = "Downlink";
	private static final String TAG_LISTTRAFFIC_UPLINK = "listTraffic_Uplink";
	private static final String TAG_LISTTRAFFIC_DOWNLINK = "listTraffic_downlink";
	private static final String TAG_RECOPENT = "recOpenT";
	private static final String TAG_DURATION = "duration";
	private static final String TAG_CAUSE = "cause";
	private static final String TAG_DIAGN = "diagn";
	private static final String TAG_RECSEQNUM = "recSeqNum";
	private static final String TAG_NODEID = "nodeID";
	private static final String TAG_RECEXT = "recExt";
	private static final String TAG_LOCALSEQNUM = "localSeqNum";
	private static final String TAG_APNSMODE = "apnSMode";
	private static final String TAG_MSISDN = "MSISDN";
	private static final String TAG_CHARGCHAR = "chargChar";
	private static final String TAG_CHCHSMODE = "chChSMode";
	private static final String TAG_IMSSIGNALCON = "iMSsignalCon";
	private static final String TAG_EXTCHARGID = "extChargID";
	private static final String TAG_NODEPLMNID = "NodePLMNId";
	private static final String TAG_PSFURNISHCHARG = "pSFurnishCharg";
	private static final String TAG_IMEISV = "IMEISV";
	private static final String TAG_RATTYPE = "rATType";
	private static final String TAG_MSTZONE = "mSTZone";
	private static final String TAG_USERLOCATION = "userLocation";
	private static final String TAG_PLMN = "PLMN";
	private static final String TAG_TAI = "TAI";
	private static final String TAG_ECGI = "ECGI";
	private static final String TAG_CAMELCHARG = "cAMELCharg";
	private static final String TAG_NODETYPE = "NodeType";
	private static final String TAG_MNNAI = "MNNAI";
	private static final String TAG_PGWPLMNID = "pGWPLMNId";
	private static final String TAG_START = "start";
	private static final String TAG_STOPT = "stopT";
	private static final String TAG_GPP2MEID = "gpp2MEID";
	private static final String TAG_PDNCONCHARGID = "pDNConChargID";
	private static final String TAG_IMSIUNAUTHFLAG = "iMSIunauthFlag";
	private static final String TAG_USERCSG = "userCSG";
	private static final String TAG_GPP2USERLOCATION = "GPP2UserLocation";
	private static final String TAG_SID = "SID";
	private static final String TAG_NID = "NID";
	private static final String TAG_CELLID = "CellID";
	private static final String TAG_PDPPDNADDREXT = "PDPPDNAddrExt";
	private static final String TAG_DADDRFLAGEXT = "dAddrFlagExt";

	public PgwXMLByString() {
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
		if (line.contains(beginContent)) {
			this.xmlFlag = true;
			// clear up,deal next block
			this.xmlList.clear();
		} else if (line.contains(endContent)) {
			this.xmlFlag = false;
			this.xmlList.add(line);

			try {
				return getContent(xmlList, invalidRecords);
			} catch (ETLException e) {
				throw new ETLException(e);
			}
		}

		if (line.contains(benginListService)) {
			this.listServiceFlag = true;
		} else if (line.contains(endListService)) {
			this.listServiceFlag = false;
		}

		// if flag is true,append block content to StringBuffer
		// if xml is <listService> block,don't add into the list
		if (this.xmlFlag == true && this.listServiceFlag == false) {
			this.xmlList.add(line);
		}
		return null;
	}

	private ETLData getContent(List<String> xmlList, Set<String> invalidRecords) {
		if (xmlList == null || xmlList.size() <= 0) {
			return null;
		}

		try {
			ETLData data = new ETLData();

			String recType = this.getValueFromList(xmlList, TAG_RECTYPE);
			data.addData(TAG_RECTYPE, recType);
			String imsi = this.getValueFromList(xmlList, TAG_IMSI);
			data.addData(TAG_IMSI, imsi);
			String pGWAddr = this.getValueFromList(xmlList, TAG_PGWADDR);
			data.addData(TAG_PGWADDR, pGWAddr);
			String chargID = this.getValueFromList(xmlList, TAG_CHARGID);
			data.addData(TAG_CHARGID, chargID);
			String nodeAddr = this.getValueFromList(xmlList, TAG_NODEADDR);
			data.addData(TAG_NODEADDR, nodeAddr);
			String apn = this.getValueFromList(xmlList, TAG_APN);
			data.addData(TAG_APN, apn);
			String pdpPDNType = this.getValueFromList(xmlList, TAG_PDPPDNTYPE);
			data.addData(TAG_PDPPDNTYPE, pdpPDNType);
			String pdppdnAddr = this.getValueFromList(xmlList, TAG_PDPPDNADDR);
			data.addData(TAG_PDPPDNADDR, pdppdnAddr);
			String dAddrFlag = this.getValueFromList(xmlList, TAG_DADDRFLAG);
			data.addData(TAG_DADDRFLAG, dAddrFlag);

			// listTraffic
			String listTraffic_Uplink = this.sumValueFromList(xmlList,
					TAG_UPLINK);
			data.addData(TAG_LISTTRAFFIC_UPLINK, listTraffic_Uplink);
			String listTraffic_Downlink = this.sumValueFromList(xmlList,
					TAG_DOWNLINK);
			data.addData(TAG_LISTTRAFFIC_DOWNLINK, listTraffic_Downlink);

			String recOpenT = this.getValueFromList(xmlList, TAG_RECOPENT);
			data.addData(TAG_RECOPENT, recOpenT);
			String duration = this.getValueFromList(xmlList, TAG_DURATION);
			data.addData(TAG_DURATION, duration);
			String cause = this.getValueFromList(xmlList, TAG_CAUSE);
			data.addData(TAG_CAUSE, cause);
			String diagn = this.getValueFromList(xmlList, TAG_DIAGN);
			data.addData(TAG_DIAGN, diagn);
			String recSeqNum = this.getValueFromList(xmlList, TAG_RECSEQNUM);
			data.addData(TAG_RECSEQNUM, recSeqNum);
			String nodeID = this.getValueFromList(xmlList, TAG_NODEID);
			data.addData(TAG_NODEID, nodeID);
			String recExt = this.getValueFromList(xmlList, TAG_RECEXT);
			data.addData(TAG_RECEXT, recExt);
			String localSeqNum = this
					.getValueFromList(xmlList, TAG_LOCALSEQNUM);
			data.addData(TAG_LOCALSEQNUM, localSeqNum);
			String apnSMode = this.getValueFromList(xmlList, TAG_APNSMODE);
			data.addData(TAG_APNSMODE, apnSMode);
			String msisdn = this.getValueFromList(xmlList, TAG_MSISDN);
			data.addData(TAG_MSISDN, msisdn);
			String chargChar = this.getValueFromList(xmlList, TAG_CHARGCHAR);
			data.addData(TAG_CHARGCHAR, chargChar);
			String chChSMode = this.getValueFromList(xmlList, TAG_CHCHSMODE);
			data.addData(TAG_CHCHSMODE, chChSMode);
			String iMSsignalCon = this.getValueFromList(xmlList,
					TAG_IMSSIGNALCON);
			data.addData(TAG_IMSSIGNALCON, iMSsignalCon);
			String extChargID = this.getValueFromList(xmlList, TAG_EXTCHARGID);
			data.addData(TAG_EXTCHARGID, extChargID);
			String todePLMNId = this.getValueFromList(xmlList, TAG_NODEPLMNID);
			data.addData(TAG_NODEPLMNID, todePLMNId);
			String pSFurnishCharg = this.getValueFromList(xmlList,
					TAG_PSFURNISHCHARG);
			data.addData(TAG_PSFURNISHCHARG, pSFurnishCharg);
			String imeisv = this.getValueFromList(xmlList, TAG_IMEISV);
			data.addData(TAG_IMEISV, imeisv);
			String rATType = this.getValueFromList(xmlList, TAG_RATTYPE);
			data.addData(TAG_RATTYPE, rATType);
			String mSTZone = this.getValueFromList(xmlList, TAG_MSTZONE);
			data.addData(TAG_MSTZONE, mSTZone);
			String userLocation = this.getLastValueFromList(xmlList,
					TAG_USERLOCATION);
			data.addData(TAG_USERLOCATION, userLocation);

			this.splitUserLocation(userLocation, data);

			String cAMELCharg = this.getValueFromList(xmlList, TAG_CAMELCHARG);
			data.addData(TAG_CAMELCHARG, cAMELCharg);
			String nodeType = this.getValueFromList(xmlList, TAG_NODETYPE);
			data.addData(TAG_NODETYPE, nodeType);
			String mnnai = this.getValueFromList(xmlList, TAG_MNNAI);
			data.addData(TAG_MNNAI, mnnai);
			String pGWPLMNId = this.getValueFromList(xmlList, TAG_PGWPLMNID);
			data.addData(TAG_PGWPLMNID, pGWPLMNId);
			String start = this.getValueFromList(xmlList, TAG_START);
			data.addData(TAG_START, start);
			String stopT = this.getValueFromList(xmlList, TAG_STOPT);
			data.addData(TAG_STOPT, stopT);
			String gpp2MEID = this.getValueFromList(xmlList, TAG_GPP2MEID);
			data.addData(TAG_GPP2MEID, gpp2MEID);
			String pDNConChargID = this.getValueFromList(xmlList,
					TAG_PDNCONCHARGID);
			data.addData(TAG_PDNCONCHARGID, pDNConChargID);
			String iMSIunauthFlag = this.getValueFromList(xmlList,
					TAG_IMSIUNAUTHFLAG);
			data.addData(TAG_IMSIUNAUTHFLAG, iMSIunauthFlag);
			String userCSG = this.getValueFromList(xmlList, TAG_USERCSG);
			data.addData(TAG_USERCSG, userCSG);
			String gpp2UserLocation = this.getValueFromList(xmlList,
					TAG_GPP2USERLOCATION);
			data.addData(TAG_GPP2USERLOCATION, gpp2UserLocation);

			this.splitGpp2UserLocation(gpp2UserLocation, data);

			String pdppdnAddrExt = this.getValueFromList(xmlList,
					TAG_PDPPDNADDREXT);
			data.addData(TAG_PDPPDNADDREXT, pdppdnAddrExt);
			String dAddrFlagExt = this.getValueFromList(xmlList,
					TAG_DADDRFLAGEXT);
			data.addData(TAG_DADDRFLAGEXT, dAddrFlagExt);

			if (this.fieldIds != null && this.fieldIds.size() > 0) {
				for (String fieldName : data.getFieldNames()) {
					// check data field
					if (this.fieldIds.contains(fieldName)) {
						Field field = super.getFieldById(fieldName);
						String fieldValue = data.getData().get(fieldName);
						super.fieldValidate(field, fieldValue,
								xmlList.toString(), invalidRecords);
					}
				}
			}

			return data;

		} catch (ETLException e) {
			throw new ETLException(e);
		}

	}

	// split gpp2UserLocation to SID,NID,CellID
	private void splitGpp2UserLocation(String gpp2UserLocation, ETLData data) throws ETLException{
		if (gpp2UserLocation == null || gpp2UserLocation.trim().length() <= 0) {
			data.addData(TAG_SID, "");
			data.addData(TAG_NID, "");
			data.addData(TAG_CELLID, "");
			return;
		}
		// split gpp2UserLocation,every two lengths add into array
		try {
			List<String> list = getList(gpp2UserLocation.substring(2), 2);

			String sid = getIDByList(list.subList(0, 4));
			data.addData(TAG_SID, sid);

			String nid = getIDByList(list.subList(4, 8));
			data.addData(TAG_NID, nid);

			String cellId = getIDByList(list.subList(8, 12));
			data.addData(TAG_CELLID, cellId);
		} catch (Exception e) {
			throw new ETLException("gpp2UserLocation:" + gpp2UserLocation + " split error");
		}
	}

	// split userLocation to PLMN,TAI,ECGI
	private void splitUserLocation(String userLocation, ETLData data) throws ETLException{
		if (userLocation == null || userLocation.trim().length() <= 0) {
			data.addData(TAG_PLMN, "");
			data.addData(TAG_TAI, "");
			data.addData(TAG_ECGI, "");
			return;
		}
		// split userLocation,every two lengths add into array
		try {
			List<String> list = getList(userLocation.substring(4), 2);

			String plmn = getPLMN(list.subList(0, 3));
			data.addData(TAG_PLMN, plmn);

			String tai = getTAIOrECGI(list.subList(3, 5));
			data.addData(TAG_TAI, tai);

			String ecgi = getTAIOrECGI(list.subList(8, 12));
			data.addData(TAG_ECGI, ecgi);
		} catch (Exception e) {
			throw new ETLException("userLocation:" + userLocation + " split error");
		}
	}

	private List<String> getList(String str, int length) {
		if (StringUtils.isBlank(str)) {
			return null;
		}

		List<String> retList = new ArrayList<String>();
		while (str.length() > length) {
			retList.add(str.substring(0, length));
			str = str.substring(length);
		}

		if (str.length() > 0) {
			retList.add(str);
		}

		return retList;
	}

	private String getPLMN(List<String> list) {
		if (list == null || list.size() <= 0) {
			return "";
		}

		StringBuffer sb = new StringBuffer();
		for (String str : list) {
			sb.append(new StringBuffer(str).reverse());
		}

		String ret = sb.toString().replace("F", "");
		return ret;
	}

	private static String getTAIOrECGI(List<String> list) {
		if (list == null) {
			return "";
		}

		StringBuffer sb = new StringBuffer();
		for (String str : list) {
			sb.append(str);
		}

		// convert hex to decimal
		return Long.toString(Long.parseLong(sb.toString(), 16));
	}

	private String getIDByList(List<String> list) {
		if (list == null || list.size() <= 0) {
			return "";
		}

		StringBuffer sb = new StringBuffer();
		for (String str : list) {
			// transform ascii code
			sb.append((char) Integer.parseInt(str, 16));
		}

		// convert hex to decimal
		return Long.toString(Long.parseLong(sb.toString(), 16));
	}

	private String getLastValueFromList(List<String> list, String tagName) {
		if (list == null || list.size() == 0) {
			return null;
		}

		int i = 0;
		Map<Integer, String> tempMap = new HashMap<Integer, String>();
		for (String str : list) {
			if (str.indexOf(tagName) != -1) {
				tempMap.put(++i, getValueByTagName(str, tagName));
			}
		}

		return tempMap.get(i);
	}

	private String sumValueFromList(List<String> list, String tagName) {
		if (list == null || list.size() == 0) {
			return null;
		}

		int sum = 0;
		for (String str : list) {
			if (str.indexOf("<" + tagName + ">") != -1) {
				sum += Integer.parseInt(getValueByTagName(str, tagName));
			}
		}

		return String.valueOf(sum);
	}

	private String getValueFromList(List<String> list, String tagName) {
		if (list == null || list.size() == 0) {
			return null;
		}

		String ret = null;
		for (String str : list) {
			if (str.indexOf("<" + tagName + ">") != -1) {
				ret = getValueByTagName(str, tagName);
			}
		}

		return ret;
	}

	private String getValueByTagName(String str, String tagName) {
		if (StringUtils.isBlank(str) || StringUtils.isBlank(tagName)) {
			return null;
		}

		String ret = null;
		int index = str.indexOf(tagName);
		if (index != -1) {
			int begin = index + tagName.length();
			int end = str.lastIndexOf("<");
			ret = str.substring(begin + 1, end);
		}

		return ret;
	}

}

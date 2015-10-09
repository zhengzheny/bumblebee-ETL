package com.gsta.bigdata.etl.localFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.LoadException;
import com.gsta.bigdata.etl.core.process.LocalFileProcess;
import com.gsta.bigdata.utils.XmlTools;

/**
 * deal pgw xml source data file
 * 
 * @author Shine
 *
 */
public class PgwXMLHandler extends AbstractHandler {
	private StringBuffer xmlBuffer = new StringBuffer();

	private boolean xmlFlag = false;

	private String beginContent = "<Content>";
	private String endContent = "</Content>";

	private static final String PATH_RECTYPE = "/Content/recType";
	private static final String PATH_IMSI = "/Content/IMSI";
	private static final String PATH_PGWADDR = "/Content/pGWAddr";
	private static final String PATH_CHARGID = "/Content/chargID";
	private static final String PATH_NODEADDR = "/Content/NodeAddr";
	private static final String PATH_APN = "/Content/APN";
	private static final String PATH_PDPPDNTYPE = "/Content/pdpPDNType";
	private static final String PATH_PDPPDNADDR = "/Content/PDPPDNAddr";
	private static final String PATH_DADDRFLAG = "/Content/dAddrFlag";
	private static final String PATH_UPLINK = "/Content/listTraffic/Uplink";
	private static final String PATH_DOWNLINK = "/Content/listTraffic/Downlink";
	private static final String PATH_RECOPENT = "/Content/recOpenT";
	private static final String PATH_DURATION = "/Content/duration";
	private static final String PATH_CAUSE = "/Content/cause";
	private static final String PATH_DIAGN = "/Content/diagn";
	private static final String PATH_RECSEQNUM = "/Content/recSeqNum";
	private static final String PATH_NODEID = "/Content/nodeID";
	private static final String PATH_RECEXT = "/Content/recExt";
	private static final String PATH_LOCALSEQNUM = "/Content/localSeqNum";
	private static final String PATH_APNSMODE = "/Content/apnSMode";
	private static final String PATH_MSISDN = "/Content/MSISDN";
	private static final String PATH_CHARGCHAR = "/Content/chargChar";
	private static final String PATH_CHCHSMODE = "/Content/chChSMode";
	private static final String PATH_IMSSIGNALCON = "/Content/iMSsignalCon";
	private static final String PATH_EXTCHARGID = "/Content/extChargID";
	private static final String PATH_NODEPLMNID = "/Content/NodePLMNId";
	private static final String PATH_PSFURNISHCHARG = "/Content/pSFurnishCharg";
	private static final String PATH_IMEISV = "/Content/IMEISV";
	private static final String PATH_RATTYPE = "/Content/rATType";
	private static final String PATH_MSTZONE = "/Content/mSTZone";
	private static final String PATH_USERLOCATION = "/Content/userLocation";
	private static final String PATH_CAMELCHARG = "/Content/cAMELCharg";
	private static final String PATH_NODETYPE = "/Content/NodeType";
	private static final String PATH_MNNAI = "/Content/MNNAI";
	private static final String PATH_PGWPLMNID = "/Content/pGWPLMNId";
	private static final String PATH_START = "/Content/start";
	private static final String PATH_STOPT = "/Content/stopT";
	private static final String PATH_GPP2MEID = "/Content/gpp2MEID";
	private static final String PATH_PDNCONCHARGID = "/Content/pDNConChargID";
	private static final String PATH_IMSIUNAUTHFLAG = "/Content/iMSIunauthFlag";
	private static final String PATH_USERCSG = "/Content/userCSG";
	private static final String PATH_GPP2USERLOCATION = "/Content/GPP2UserLocation";
	private static final String PATH_PDPPDNADDREXT = "/Content/PDPPDNAddrExt";
	private static final String PATH_DADDRFLAGEXT = "/Content/dAddrFlagExt";

	private static final String TAG_RECTYPE = "recType";
	private static final String TAG_IMSI = "IMSI";
	private static final String TAG_PGWADDR = "pGWAddr";
	private static final String TAG_CHARGID = "chargID";
	private static final String TAG_NODEADDR = "NodeAddr";
	private static final String TAG_APN = "APN";
	private static final String TAG_PDPPDNTYPE = "pdpPDNType";
	private static final String TAG_PDPPDNADDR = "PDPPDNAddr";
	private static final String TAG_DADDRFLAG = "dAddrFlag";
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

	private Logger logger = LoggerFactory.getLogger(getClass());

	public PgwXMLHandler(LocalFileProcess process) {
		super(process);
	}

	@Override
	protected void _handle(String line) throws ETLException {
		if (line.contains(beginContent)) {
			this.xmlFlag = true;
			// clear up,deal next block
			this.xmlBuffer.delete(0, this.xmlBuffer.toString().length());
		} else if (line.contains(endContent)) {
			this.xmlFlag = false;
			this.xmlBuffer.append(line);
			String xml = this.xmlBuffer.toString();

			try {
				getContent(xml);
			} catch (ETLException e) {
				logger.error(e.getMessage());

				super.errorRecords.add(e.getMessage());
				if (super.errorRecords.size() >= super.errorRecordThreshold) {
					super.writeFiles(super.errorOutStream, super.errorRecords,
							super.errorFileName);
				}

				super.errorCount++;
			}
		}

		// if flag is true,append block content to StringBuffer
		if (this.xmlFlag) {
			this.xmlBuffer.append(line);
		}
	}

	private void getContent(String xml) {
		if (xml == null) {
			return;
		}

		try {
			Document doc;
			try {
				doc = XmlTools.loadFromContent(xml);
			} catch (ParserConfigurationException | SAXException | IOException e) {
				throw new LoadException(e);
			}

			ETLData data = new ETLData();

			String recType = XmlTools.getNodeValue(doc, PATH_RECTYPE);
			data.addData(TAG_RECTYPE, recType);
			String imsi = XmlTools.getNodeValue(doc, PATH_IMSI);
			data.addData(TAG_IMSI, imsi);
			String pGWAddr = XmlTools.getNodeValue(doc, PATH_PGWADDR);
			data.addData(TAG_PGWADDR, pGWAddr);
			String chargID = XmlTools.getNodeValue(doc, PATH_CHARGID);
			data.addData(TAG_CHARGID, chargID);
			String nodeAddr = XmlTools.getNodeValue(doc, PATH_NODEADDR);
			data.addData(TAG_NODEADDR, nodeAddr);
			String apn = XmlTools.getNodeValue(doc, PATH_APN);
			data.addData(TAG_APN, apn);
			String pdpPDNType = XmlTools.getNodeValue(doc, PATH_PDPPDNTYPE);
			data.addData(TAG_PDPPDNTYPE, pdpPDNType);
			String pdppdnAddr = XmlTools.getNodeValue(doc, PATH_PDPPDNADDR);
			data.addData(TAG_PDPPDNADDR, pdppdnAddr);
			String dAddrFlag = XmlTools.getNodeValue(doc, PATH_DADDRFLAG);
			data.addData(TAG_DADDRFLAG, dAddrFlag);

			// listTraffic
			String listTraffic_Uplink = XmlTools.sumNodeValue(doc, PATH_UPLINK);
			data.addData(TAG_LISTTRAFFIC_UPLINK, listTraffic_Uplink);
			String listTraffic_Downlink = XmlTools.sumNodeValue(doc,
					PATH_DOWNLINK);
			data.addData(TAG_LISTTRAFFIC_DOWNLINK, listTraffic_Downlink);

			String recOpenT = XmlTools.getNodeValue(doc, PATH_RECOPENT);
			data.addData(TAG_RECOPENT, recOpenT);
			String duration = XmlTools.getNodeValue(doc, PATH_DURATION);
			data.addData(TAG_DURATION, duration);
			String cause = XmlTools.getNodeValue(doc, PATH_CAUSE);
			data.addData(TAG_CAUSE, cause);
			String diagn = XmlTools.getNodeValue(doc, PATH_DIAGN);
			data.addData(TAG_DIAGN, diagn);
			String recSeqNum = XmlTools.getNodeValue(doc, PATH_RECSEQNUM);
			data.addData(TAG_RECSEQNUM, recSeqNum);
			String nodeID = XmlTools.getNodeValue(doc, PATH_NODEID);
			data.addData(TAG_NODEID, nodeID);
			String recExt = XmlTools.getNodeValue(doc, PATH_RECEXT);
			data.addData(TAG_RECEXT, recExt);
			String localSeqNum = XmlTools.getNodeValue(doc, PATH_LOCALSEQNUM);
			data.addData(TAG_LOCALSEQNUM, localSeqNum);
			String apnSMode = XmlTools.getNodeValue(doc, PATH_APNSMODE);
			data.addData(TAG_APNSMODE, apnSMode);
			String msisdn = XmlTools.getNodeValue(doc, PATH_MSISDN);
			data.addData(TAG_MSISDN, msisdn);
			String chargChar = XmlTools.getNodeValue(doc, PATH_CHARGCHAR);
			data.addData(TAG_CHARGCHAR, chargChar);
			String chChSMode = XmlTools.getNodeValue(doc, PATH_CHCHSMODE);
			data.addData(TAG_CHCHSMODE, chChSMode);
			String iMSsignalCon = XmlTools.getNodeValue(doc, PATH_IMSSIGNALCON);
			data.addData(TAG_IMSSIGNALCON, iMSsignalCon);
			String extChargID = XmlTools.getNodeValue(doc, PATH_EXTCHARGID);
			data.addData(TAG_EXTCHARGID, extChargID);
			String todePLMNId = XmlTools.getNodeValue(doc, PATH_NODEPLMNID);
			data.addData(TAG_NODEPLMNID, todePLMNId);
			String pSFurnishCharg = XmlTools.getNodeValue(doc,
					PATH_PSFURNISHCHARG);
			data.addData(TAG_PSFURNISHCHARG, pSFurnishCharg);
			String imeisv = XmlTools.getNodeValue(doc, PATH_IMEISV);
			data.addData(TAG_IMEISV, imeisv);
			String rATType = XmlTools.getNodeValue(doc, PATH_RATTYPE);
			data.addData(TAG_RATTYPE, rATType);
			String mSTZone = XmlTools.getNodeValue(doc, PATH_MSTZONE);
			data.addData(TAG_MSTZONE, mSTZone);
			String userLocation = XmlTools.getNodeValue(doc, PATH_USERLOCATION);
			data.addData(TAG_USERLOCATION, userLocation);

			this.splitUserLocation(userLocation, data);

			String cAMELCharg = XmlTools.getNodeValue(doc, PATH_CAMELCHARG);
			data.addData(TAG_CAMELCHARG, cAMELCharg);
			String nodeType = XmlTools.getNodeValue(doc, PATH_NODETYPE);
			data.addData(TAG_NODETYPE, nodeType);
			String mnnai = XmlTools.getNodeValue(doc, PATH_MNNAI);
			data.addData(TAG_MNNAI, mnnai);
			String pGWPLMNId = XmlTools.getNodeValue(doc, PATH_PGWPLMNID);
			data.addData(TAG_PGWPLMNID, pGWPLMNId);
			String start = XmlTools.getNodeValue(doc, PATH_START);
			data.addData(TAG_START, start);
			String stopT = XmlTools.getNodeValue(doc, PATH_STOPT);
			data.addData(TAG_STOPT, stopT);
			String gpp2MEID = XmlTools.getNodeValue(doc, PATH_GPP2MEID);
			data.addData(TAG_GPP2MEID, gpp2MEID);
			String pDNConChargID = XmlTools.getNodeValue(doc,
					PATH_PDNCONCHARGID);
			data.addData(TAG_PDNCONCHARGID, pDNConChargID);
			String iMSIunauthFlag = XmlTools.getNodeValue(doc,
					PATH_IMSIUNAUTHFLAG);
			data.addData(TAG_IMSIUNAUTHFLAG, iMSIunauthFlag);
			String userCSG = XmlTools.getNodeValue(doc, PATH_USERCSG);
			data.addData(TAG_USERCSG, userCSG);
			String gpp2UserLocation = XmlTools.getNodeValue(doc,
					PATH_GPP2USERLOCATION);
			data.addData(TAG_GPP2USERLOCATION, gpp2UserLocation);
			
			this.splitGpp2UserLocation(gpp2UserLocation,data);
			
			String pdppdnAddrExt = XmlTools.getNodeValue(doc,
					PATH_PDPPDNADDREXT);
			data.addData(TAG_PDPPDNADDREXT, pdppdnAddrExt);
			String dAddrFlagExt = XmlTools.getNodeValue(doc, PATH_DADDRFLAGEXT);
			data.addData(TAG_DADDRFLAGEXT, dAddrFlagExt);

			super.process.verifyFields(data);

			super.process.onTransform(data, SCOPE);

			String outputValue = super.process.getOutputValue(data);
			super.queue.add(outputValue);
			if (super.queue.size() >= super.recordThreshold) {
				super.writeFiles(super.queueOutStream, super.queue,
						super.queueFileName);
			}

			super.recordCount++;

		} catch (ETLException e) {
			super.errorRecords.add(e.getMessage());
			super.errorCount++;

			if (super.errorRecords.size() >= super.errorRecordThreshold) {
				super.writeFiles(super.errorOutStream, super.errorRecords,
						super.errorFileName);
			}
		} catch (XPathExpressionException e) {
			throw new ETLException(e);
		} catch(LoadException e){
			logger.warn(e.getMessage());
		}

	}
	
	// split gpp2UserLocation to SID,NID,CellID
	private void splitGpp2UserLocation(String gpp2UserLocation, ETLData data) {
		if(gpp2UserLocation == null || gpp2UserLocation.trim().length() <=0){
			data.addData(TAG_SID, "");
			data.addData(TAG_NID, "");
			data.addData(TAG_CELLID, "");
			return;
		}
		//split gpp2UserLocation,every two lengths add into array
		List<String> list = getList(gpp2UserLocation.substring(2), 2);
		
		String sid = getIDByList(list.subList(0, 4));
		data.addData(TAG_SID, sid);
		
		String nid = getIDByList(list.subList(4, 8));
		data.addData(TAG_NID, nid);
		
		String cellId = getIDByList(list.subList(8, 12));
		data.addData(TAG_CELLID, cellId);
	}

	//split userLocation to PLMN,TAI,ECGI
	private void splitUserLocation(String userLocation, ETLData data) {
		if(userLocation == null || userLocation.trim().length() <=0){
			data.addData(TAG_PLMN, "");
			data.addData(TAG_TAI, "");
			data.addData(TAG_ECGI, "");
			return;
		}
		//split userLocation,every two lengths add into array
		List<String> list = getList(userLocation.substring(4), 2);
		
		String plmn = getPLMN(list.subList(0, 3));
		data.addData(TAG_PLMN, plmn);
		
		String tai = getTAIOrECGI(list.subList(3, 5));
		data.addData(TAG_TAI, tai);
		
		String ecgi = getTAIOrECGI(list.subList(8, 12));
		data.addData(TAG_ECGI, ecgi);
	}

	private List<String> getList(String str, int length) {
		if (null == str || "".equals(str)) {
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
		if(list == null || list.size() <= 0){
			return "";
		}
		
		StringBuffer sb = new StringBuffer();
		for(String str:list){
			sb.append(new StringBuffer(str).reverse());
		}
		
		String ret = sb.toString().replace("F", "");
		return ret;
	}
	
	private static String getTAIOrECGI(List<String> list) {
		if(list == null){
			return "";
		}
		
		StringBuffer sb = new StringBuffer();
		for(String str:list){
			sb.append(str);
		}
		
		//convert hex to decimal
		return Long.toString(Long.parseLong(sb.toString(), 16));
	}
	
	private  String getIDByList(List<String> list) {
		if(list == null || list.size() <=0){
			return "";
		}
		
		StringBuffer sb = new StringBuffer();
		for(String str:list){
			//transform ascii code
			sb.append((char)Integer.parseInt(str, 16));
		}
		
		//convert hex to decimal
		return Long.toString(Long.parseLong(sb.toString(), 16));
	}

}

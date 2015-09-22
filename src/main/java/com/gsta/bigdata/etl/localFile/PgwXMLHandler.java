package com.gsta.bigdata.etl.localFile;

import java.io.IOException;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.ETLData;
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

	private String contentPath = "/Content";
	private String listTrafficPath = "/Content/listTraffic";
    // private String eQoSPath = "/Content/listTraffic/eQoS";
	// private String listServicePath = "/Content/listService";
	// private String qoSNegPath = "/Content/listService/qoSNeg";

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
	// private static final String TAG_CHCOND = "chCond";
	// private static final String TAG_CHT = "chT";
	// private static final String TAG_QCI = "qCI";
	// private static final String TAG_ARP = "aRP";
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
	private static final String TAG_CAMELCHARG = "cAMELCharg";
	// private static final String TAG_GROUP = "Group";
	// private static final String TAG_RULENAME = "RuleName";
	// private static final String TAG_LSEQNUM = "lSeqNum";
	// private static final String TAG_TFIRST = "TFirst";
	// private static final String TAG_TLAST = "TLast";
	// private static final String TAG_TUSAGE = "TUsage";
	// private static final String TAG_CONDCHANGE = "CondChange";
	// private static final String TAG_SGSNADDR = "sgsnAddr";
	// private static final String TAG_TREPO = "TRepo";
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
	private static final String TAG_PDPPDNADDREXT = "PDPPDNAddrExt";
	private static final String TAG_DADDRFLAGEXT = "dAddrFlagExt";

	private static final String FIELD_LISTTRAFFIC_UPLINK = "listTraffic_Uplink";
	private static final String FIELD_LISTTRAFFIC_DOWNLINK = "listTraffic_downlink";
	// private static final String FIELD_LISTSERVICE_UPLINK =
	// "listService_Uplink";
	// private static final String FIELD_LISTSERVICE_DOWNLINK =
	// "listService_Downlink";
	// private static final String FIELD_EQOS_QCI = "eQoS_qCI";
	// private static final String FIELD_EQOS_ARP = "eQoS_aRP";
	// private static final String FIELD_QOSNEG_QCI = "qoSNeg_qCI";
	// private static final String FIELD_QOSNEG_ARP = "qoSNeg_aRP";

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
			Node content;
			try {
				doc = XmlTools.loadFromContent(xml);
				content = XmlTools.getNodeByPath(doc, contentPath);
			} catch (ParserConfigurationException | SAXException | IOException
					| XPathExpressionException e) {
				throw new ETLException(e);
			}

			ETLData data = new ETLData();

			String recType = getNodeValueByTagName(content, TAG_RECTYPE);
			data.addData(TAG_RECTYPE, recType);
			String imsi = getNodeValueByTagName(content, TAG_IMSI);
			data.addData(TAG_IMSI, imsi);
			String pGWAddr = getNodeValueByTagName(content, TAG_PGWADDR);
			data.addData(TAG_PGWADDR, pGWAddr);
			String chargID = getNodeValueByTagName(content, TAG_CHARGID);
			data.addData(TAG_CHARGID, chargID);
			String nodeAddr = getNodeValueByTagName(content, TAG_NODEADDR);
			data.addData(TAG_NODEADDR, nodeAddr);
			String apn = getNodeValueByTagName(content, TAG_APN);
			data.addData(TAG_APN, apn);
			String pdpPDNType = getNodeValueByTagName(content, TAG_PDPPDNTYPE);
			data.addData(TAG_PDPPDNTYPE, pdpPDNType);
			String pdppdnAddr = getNodeValueByTagName(content, TAG_PDPPDNADDR);
			data.addData(TAG_PDPPDNADDR, pdppdnAddr);
			String dAddrFlag = getNodeValueByTagName(content, TAG_DADDRFLAG);
			data.addData(TAG_DADDRFLAG, dAddrFlag);

			// listTraffic
			String listTraffic_Uplink = sumNodeValueByPath(doc,
					listTrafficPath, TAG_UPLINK);
			data.addData(FIELD_LISTTRAFFIC_UPLINK, listTraffic_Uplink);
			String listTraffic_Downlink = sumNodeValueByPath(doc,
					listTrafficPath, TAG_DOWNLINK);
			data.addData(FIELD_LISTTRAFFIC_DOWNLINK, listTraffic_Downlink);
			/*
			 * String chCond = getNodeValueByPath(doc, listTrafficPath,
			 * TAG_CHCOND); data.addData(TAG_CHCOND, chCond); String chT =
			 * getNodeValueByPath(doc, listTrafficPath, TAG_CHT);
			 * data.addData(TAG_CHT, chT);
			 */
			/*
			 * String eQoS_qCI = getNodeValueByPath(doc, eQoSPath, TAG_QCI);
			 * data.addData(FIELD_EQOS_QCI, eQoS_qCI); String eQoS_aRP =
			 * getNodeValueByPath(doc, eQoSPath, TAG_ARP);
			 * data.addData(FIELD_EQOS_ARP, eQoS_aRP);
			 */

			String recOpenT = getNodeValueByTagName(content, TAG_RECOPENT);
			data.addData(TAG_RECOPENT, recOpenT);
			String duration = getNodeValueByTagName(content, TAG_DURATION);
			data.addData(TAG_DURATION, duration);
			String cause = getNodeValueByTagName(content, TAG_CAUSE);
			data.addData(TAG_CAUSE, cause);
			String diagn = getNodeValueByTagName(content, TAG_DIAGN);
			data.addData(TAG_DIAGN, diagn);
			String recSeqNum = getNodeValueByTagName(content, TAG_RECSEQNUM);
			data.addData(TAG_RECSEQNUM, recSeqNum);
			String nodeID = getNodeValueByTagName(content, TAG_NODEID);
			data.addData(TAG_NODEID, nodeID);
			String recExt = getNodeValueByTagName(content, TAG_RECEXT);
			data.addData(TAG_RECEXT, recExt);
			String localSeqNum = getNodeValueByTagName(content, TAG_LOCALSEQNUM);
			data.addData(TAG_LOCALSEQNUM, localSeqNum);
			String apnSMode = getNodeValueByTagName(content, TAG_APNSMODE);
			data.addData(TAG_APNSMODE, apnSMode);
			String msisdn = getNodeValueByTagName(content, TAG_MSISDN);
			data.addData(TAG_MSISDN, msisdn);
			String chargChar = getNodeValueByTagName(content, TAG_CHARGCHAR);
			data.addData(TAG_CHARGCHAR, chargChar);
			String chChSMode = getNodeValueByTagName(content, TAG_CHCHSMODE);
			data.addData(TAG_CHCHSMODE, chChSMode);
			String iMSsignalCon = getNodeValueByTagName(content,
					TAG_IMSSIGNALCON);
			data.addData(TAG_IMSSIGNALCON, iMSsignalCon);
			String extChargID = getNodeValueByTagName(content, TAG_EXTCHARGID);
			data.addData(TAG_EXTCHARGID, extChargID);
			String todePLMNId = getNodeValueByTagName(content, TAG_NODEPLMNID);
			data.addData(TAG_NODEPLMNID, todePLMNId);
			String pSFurnishCharg = getNodeValueByTagName(content,
					TAG_PSFURNISHCHARG);
			data.addData(TAG_PSFURNISHCHARG, pSFurnishCharg);
			String imeisv = getNodeValueByTagName(content, TAG_IMEISV);
			data.addData(TAG_IMEISV, imeisv);
			String rATType = getNodeValueByTagName(content, TAG_RATTYPE);
			data.addData(TAG_RATTYPE, rATType);
			String mSTZone = getNodeValueByTagName(content, TAG_MSTZONE);
			data.addData(TAG_MSTZONE, mSTZone);
			String userLocation = getNodeValueByTagName(content,
					TAG_USERLOCATION);
			data.addData(TAG_USERLOCATION, userLocation);
			String cAMELCharg = getNodeValueByTagName(content, TAG_CAMELCHARG);
			data.addData(TAG_CAMELCHARG, cAMELCharg);

			// listService
			/*
			 * String group = getNodeValueByPath(doc, listServicePath,
			 * TAG_GROUP); data.addData(TAG_GROUP, group); String ruleName =
			 * getNodeValueByPath(doc, listServicePath, TAG_RULENAME);
			 * data.addData(TAG_RULENAME, ruleName); String lSeqNum =
			 * getNodeValueByPath(doc, listServicePath, TAG_LSEQNUM);
			 * data.addData(TAG_LSEQNUM, lSeqNum); String tFirst =
			 * getNodeValueByPath(doc, listServicePath, TAG_TFIRST);
			 * data.addData(TAG_TFIRST, tFirst); String tLast =
			 * getNodeValueByPath(doc, listServicePath, TAG_TLAST);
			 * data.addData(TAG_TLAST, tLast); String tUsage =
			 * getNodeValueByPath(doc, listServicePath, TAG_TUSAGE);
			 * data.addData(TAG_TUSAGE, tUsage); String condChange =
			 * getNodeValueByPath(doc, listServicePath, TAG_CONDCHANGE);
			 * data.addData(TAG_CONDCHANGE, condChange); String qoSNeg_qCI =
			 * getNodeValueByPath(doc, qoSNegPath, TAG_QCI);
			 * data.addData(FIELD_QOSNEG_QCI, qoSNeg_qCI); String qoSNeg_aRP =
			 * getNodeValueByPath(doc, qoSNegPath, TAG_ARP);
			 * data.addData(FIELD_QOSNEG_ARP, qoSNeg_aRP); String sgsnAddr =
			 * getNodeValueByPath(doc, listServicePath, TAG_SGSNADDR);
			 * data.addData(TAG_SGSNADDR, sgsnAddr); String listService_Uplink =
			 * getNodeValueByPath(doc, listServicePath, TAG_UPLINK);
			 * data.addData(FIELD_LISTSERVICE_UPLINK, listService_Uplink);
			 * String listService_Downlink = getNodeValueByPath(doc,
			 * listServicePath, TAG_DOWNLINK);
			 * data.addData(FIELD_LISTSERVICE_DOWNLINK, listService_Downlink);
			 * String tRepo = getNodeValueByPath(doc, listServicePath,
			 * TAG_TREPO); data.addData(TAG_TREPO, tRepo);
			 */

			String nodeType = getNodeValueByTagName(content, TAG_NODETYPE);
			data.addData(TAG_NODETYPE, nodeType);
			String mnnai = getNodeValueByTagName(content, TAG_MNNAI);
			data.addData(TAG_MNNAI, mnnai);
			String pGWPLMNId = getNodeValueByTagName(content, TAG_PGWPLMNID);
			data.addData(TAG_PGWPLMNID, pGWPLMNId);
			String start = getNodeValueByTagName(content, TAG_START);
			data.addData(TAG_START, start);
			String stopT = getNodeValueByTagName(content, TAG_STOPT);
			data.addData(TAG_STOPT, stopT);
			String gpp2MEID = getNodeValueByTagName(content, TAG_GPP2MEID);
			data.addData(TAG_GPP2MEID, gpp2MEID);
			String pDNConChargID = getNodeValueByTagName(content,
					TAG_PDNCONCHARGID);
			data.addData(TAG_PDNCONCHARGID, pDNConChargID);
			String iMSIunauthFlag = getNodeValueByTagName(content,
					TAG_IMSIUNAUTHFLAG);
			data.addData(TAG_IMSIUNAUTHFLAG, iMSIunauthFlag);
			String userCSG = getNodeValueByTagName(content, TAG_USERCSG);
			data.addData(TAG_USERCSG, userCSG);
			String gpp2UserLocation = getNodeValueByTagName(content,
					TAG_GPP2USERLOCATION);
			data.addData(TAG_GPP2USERLOCATION, gpp2UserLocation);
			String pdppdnAddrExt = getNodeValueByTagName(content,
					TAG_PDPPDNADDREXT);
			data.addData(TAG_PDPPDNADDREXT, pdppdnAddrExt);
			String dAddrFlagExt = getNodeValueByTagName(content,
					TAG_DADDRFLAGEXT);
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
		}

	}
	
	//<Content>
	//	<recType>PGW-CDR</recType>
	//	<IMSI>460110416142930</IMSI>
	//</Content>
	private String getNodeValueByTagName(Node content, String tagName) {
		String value = "";

		try {
			Element node = XmlTools.getFirstChildByTagName((Element) content,
					tagName);
			value = XmlTools.getNodeValue(node);
		} catch (XPathExpressionException e) {
			throw new ETLException(e);
		}

		return value;
	}
	
	//<Content>
	//	<listTraffic>
	//		<Uplink>0</Uplink>
	//		<Downlink>0</Downlink>
	//		<Uplink>342</Uplink>
	//		<Downlink>104</Downlink>
	//	</listTraffic>
	//</Content>
	//sum the same tag value
	private String sumNodeValueByPath(Document doc, String path, String tagName) {
		long sum = 0L;

		try {
			Node node = XmlTools.getNodeByPath(doc, path);
			List<Element> list = XmlTools.getChildrenByTagName((Element) node,
					tagName);

			for (Element element : list) {
				String value = XmlTools.getNodeValue(element);
				sum += Long.parseLong(value);
			}
		} catch (XPathExpressionException e) {
			throw new ETLException(e);
		}

		return String.valueOf(sum);
	}

}

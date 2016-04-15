package com.gsta.bigdata.etl.core.source;

import java.util.List;
import java.util.Set;

import javax.xml.xpath.XPathExpressionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.ChildrenTag;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.utils.SourceXmlTool;
import com.gsta.bigdata.utils.XmlTools;

/**
 *  source file
<?xml version="1.0" encoding="UTF-8"?>
<mediaData><Message><PhoneInfo><IMSI>460031267756914</IMSI>
<MEID>A0000055D626A5</MEID>
<PhoneType>HUAWEI C8818</PhoneType>
<OSVersion></OSVersion>
<BaseBand></BaseBand>
<Kernel></Kernel>
<InnerVersion></InnerVersion>
<RamUsage>55</RamUsage>
<CpuUsage>19</CpuUsage>
</PhoneInfo>
<PositionInfo><Longitude>113.25644</Longitude>
<Latitude>23.22297</Latitude>
<LocationDesc></LocationDesc>
<Province></Province>
<City></City>
</PositionInfo>
<NetInfo><NetType>CDMA 1X</NetType>
<APN></APN>
<CdmaSid>13828</CdmaSid>
<CdmaNid>2</CdmaNid>
<CdmaBsid></CdmaBsid>
<CdmadBm>-94.00</CdmadBm>
<LteCi></LteCi>
<LtePci></LtePci>
<LteTac></LteTac>
<LteRsrp></LteRsrp>
<LteSinr></LteSinr>
<InnerIP>192.168.0.108</InnerIP>
<OuterIP></OuterIP>
</NetInfo>
<TestResult><VideoName>ÌÚÑ¶</VideoName>
<VideoURL>http://m.v.qq.com</VideoURL>
<VideoIP></VideoIP>
<VideoTestTime>2016-03-01 00:16:45.164</VideoTestTime>
<VideoAvgSpeed>206.19</VideoAvgSpeed>
<VideoPeakSpeed>209.93</VideoPeakSpeed>
<TCLASS></TCLASS>
<BufferCounter>0</BufferCounter>
<VideoSize></VideoSize>
</TestResult>
</Message>
</mediaData>
**** config file **************
<sourceMetaData type="SimpleXML">
			<tagField>IMSI,MEID,PhoneType,OSVersion,BaseBand,Kernel,InnerVersion,RamUsage,CpuUsage,Longitude,Latitude,LocationDesc,Province,City,NetType,APN,CdmaSid,CdmaNid,CdmaBsid,CdmadBm,LteCi,LtePci,LteTac,LteRsrp,LteSinr,InnerIP,OuterIP,VideoName,VideoURL,VideoIP,VideoTestTime,VideoAvgSpeed,VideoPeakSpeed,TCLASS,BufferCounter,VideoSize</tagField>
			<segmentField>Message</segmentField>
			<paths>
				<inputPath path="${inputPath}"/>
			</paths>
		</sourceMetaData>
get all data from xml file
460031267756914|A0000055D626A5|HUAWEI C8818|||||55|19|113.25644|23.22297||||CDMA 1X||13828|2||-94.00||||||192.168.0.108||ÌÚÑ¶|http://m.v.qq.com||2016-03-01 00:16:45.164|206.19|209.93||0|

 * @author tianxq
 *
 */
public class SimpleXML extends AbstractSourceMetaData {
	private static final long serialVersionUID = -7936071251070183503L;
	private ETLData etlData;
	@JsonProperty
	private String[] tags;
	@JsonProperty
	private String beginSegmentField;
	@JsonProperty
	private String endSegmentField;
	private Logger logger = LoggerFactory.getLogger(getClass());

	public SimpleXML() {
		super();
		
		super.registerChildrenTags(new ChildrenTag(
				Constants.PATH_TAG_FIELD,
				ChildrenTag.NODE));
		
		super.registerChildrenTags(new ChildrenTag(
				Constants.PATH_SEGMENT_FIELD,
				ChildrenTag.NODE));
	}

	@Override
	protected void createChildNode(Element node) throws ParseException {
		super.createChildNode(node);

		try {
			if (node.getNodeName().equals(Constants.PATH_TAG_FIELD)) {
				String fields = XmlTools.getNodeValue(node);
				if (fields != null) {
					this.tags = fields.split(",", -1);
				}
				if(this.tags == null || this.tags.length <= 0){
					throw new ParseException("tagField isn't filled...");
				}
			}

			if (node.getNodeName().equals(Constants.PATH_SEGMENT_FIELD)) {
				String value = XmlTools.getNodeValue(node);
				if(value == null){
					throw new ParseException("segmentField isn't filled...");
				}
				
				this.beginSegmentField = "<" + value + ">";
				this.endSegmentField = "</" + value + ">";
			}
		} catch (XPathExpressionException e) {
			throw new ParseException(e);
		}
	}

	@Override
	public ETLData parseLine(String line, Set<String> invalidRecords)
			throws ETLException, ValidatorException {
		if(line == null || "".equals(line)){
			return null;
		}
		
		if(this.tags == null || this.beginSegmentField == null || this.endSegmentField == null){
			return null;
		}
		
		if(line.indexOf(this.beginSegmentField) != -1){
			this.etlData = new ETLData();
		}		
		
		for(String field:this.tags){
			String tag = "<" + field + ">";
			if(line.indexOf(tag) != -1){
				String value = SourceXmlTool.getTagValue(line, field);
				//xml segment maybe cut by mapper,so etlData object isn't be initialized. 
				if(this.etlData != null){
					this.etlData.addData(field, value);
				}else{
					logger.error(line);
				}
			}
		}
		
		if(line.indexOf(this.endSegmentField) != -1){
			return this.etlData;
		}
		
		return null;
	}

	@Override
	public List<ETLData> parseLine(String line) throws ETLException,
			ValidatorException {
		//don't need
		return null;
	}

}

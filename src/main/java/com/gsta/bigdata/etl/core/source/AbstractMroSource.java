package com.gsta.bigdata.etl.core.source;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.Field;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.utils.SourceXmlTool;

/**
 * abstract mro xml source for gdnoce project
 * @author tianxq
 *
 */
public abstract class AbstractMroSource extends AbstractSourceMetaData {
	private static final long serialVersionUID = 1L;
	protected String startTime;
	protected String endTime;
	protected List<String> smrs = new ArrayList<String>();
	protected ETLData etlData = new ETLData();

	@JsonProperty
	private List<String> fieldIds = new ArrayList<String>();
	
	protected static final String ATTR_STARTTIME = "startTime";
	protected static final String ATTR_ENDTIME = "endTime";
	protected static final String ATTR_TIMESTAMP = "TimeStamp";
	protected static final String ATTR_ID = "id";
	protected static final String ATTR_MMEGROUPID = "MmeGroupId";
	protected static final String ATTR_MMEUES1APID = "MmeUeS1apId";
	protected static final String ATTR_MMECODE = "MmeCode";

	protected static final String TAG_SMR = "smr";
	protected static final String TAG_V = "v";

	protected static final String FIELD_STARTTIME = "startTime";
	protected static final String FIELD_ENDTIME = "endTime";
	protected static final String FIELD_TIMESTAMP = "TimeStamp";
	protected static final String FIELD_ENODEBID = "ENODEID";
	protected static final String FIELD_CELLID = "CELLID";
	protected static final String FIELD_MMEGROUPID = "MmeGroupId";
	protected static final String FIELD_MMEUES1APID = "MmeUeS1apId";
	protected static final String FIELD_MMECODE = "MmeCode";
	
	public AbstractMroSource(){
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
	
	protected void computeHeaderAndSmr(String line){
		if(line == null || "".equals(line)){
			return;
		}
		
		if (line.indexOf("<fileHeader") != -1) {
			this.startTime = SourceXmlTool.getAttrValue(line, ATTR_STARTTIME).replace("T", " ");
			this.endTime = SourceXmlTool.getAttrValue(line, ATTR_ENDTIME).replace("T"," ");
		}
		
		if (line.indexOf("<smr>") != -1) {
			this.smrs.clear();

			String smr = SourceXmlTool.getTagValue(line, TAG_SMR);
			if (StringUtils.isNotBlank(smr)) {
				String[] tempSmr = smr.replace(".", "_").split(" ");
				for (String temp : tempSmr) {
					this.smrs.add(temp);
				}
			}
		}
	}
	
	public abstract ETLData parseLine(String line, Set<String> invalidRecords)
			throws ETLException, ValidatorException;
	
	/**
	 * when the source field is not contain the master key
	 * @throws ETLException
	 */
	protected void verifyKeyField() throws ETLException {
		if (this.fieldIds != null && this.fieldIds.size() > 0) {
			List<String> fieldNames = this.etlData.getFieldNames();
			List<String> tempFieldIds = new ArrayList<String>();
			tempFieldIds.addAll(this.fieldIds);
			tempFieldIds.removeAll(fieldNames);
			if (tempFieldIds.size() > 0) {
				String errorMsg = tempFieldIds.toString();
				String errorCode = String.valueOf(errorMsg.hashCode());
				throw new ETLException(errorCode,
						"source miss master key field:" + errorMsg
								+ ",current smr is:" + this.smrs.toString());
			}
		}
	}
}

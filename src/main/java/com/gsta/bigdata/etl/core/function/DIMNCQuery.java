package com.gsta.bigdata.etl.core.function;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.etl.core.ShellContext;
import com.gsta.bigdata.etl.core.lookup.DIMDS;
import com.gsta.bigdata.etl.core.source.mro.DIMObj;

public class DIMNCQuery extends DIMInfoQuery {
	private static final long serialVersionUID = 8631890142829016706L;
	@JsonProperty
	private String pciField;
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	public DIMNCQuery(){
		super();
	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		super.initAttrs(element);
		
		this.pciField = super.getAttr(Constants.ATTR_PCI_INDEX);
	}

	@Override
	protected String onCalculate(Map<String, String> functionData,
			ShellContext context) throws ETLException {
		//don't need single output
		return null;
	}

	@Override
	protected Map<String, String> multiOutputOnCalculate(
			Map<String, String> functionData, ShellContext context)
			throws ETLException {
		Map<String, String> ret = new HashMap<String, String>();

		int pciIdx = -1;
		try {
			pciIdx = Integer.parseInt(functionData.get(this.pciField));
		} catch (NumberFormatException e) {
			//cat find PCI index,continue to output record
			logger.error("get " + this.pciField + " occur error:" + e.getMessage());
		}

		//first search dimensions table accord ENODEID,CELLID
		DIMObj dimObj = (DIMObj) this.table.getValue(DIMDS.getKey(functionData,super.keys));
		if (dimObj != null) {
			String[] tempKeys = dimObj.getkeyByPCIIdx(pciIdx);
			if(tempKeys != null && tempKeys.length == 2){
				//second search dimensions by PCI index eNodeBID+CELLID
				String tempKey = tempKeys[0] + DIMObj.KEY_FIELD_DELIMITER + tempKeys[1];
				dimObj = (DIMObj) this.table.getValue(tempKey);
				if(dimObj != null){
					ret.putAll(dimObj.getNCData(super.getOutputIds()));
				}
			}
		}

		return ret;
	}
}

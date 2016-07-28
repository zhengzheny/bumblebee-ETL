package com.gsta.bigdata.etl.core.function;

import java.util.Map;

import org.w3c.dom.Element;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ShellContext;

public class LTEUPUePort extends AbstractFunction {
	private static final long serialVersionUID = 6062366038084188440L;
	@JsonProperty
	private String inputField;

	public LTEUPUePort() {
		super();
	}

	@Override
	protected void initAttrs(Element element)
			throws com.gsta.bigdata.etl.core.ParseException {
		super.initAttrs(element);

		this.inputField = super.getAttr(Constants.ATTR_INPUT);
	}

	@Override
	protected String onCalculate(Map<String, String> functionData,
			ShellContext context) throws ETLException {
		if (functionData != null) {
			String uePort = functionData.get(inputField);
			try {
				long ue_port = Long.parseLong(uePort);
				if(ue_port < 0){
					ue_port = 32768 + ue_port;
				}
				return String.valueOf(ue_port);
			} catch (NumberFormatException e) {
				throw new ETLException(e);
			}
		}

		return null;
	}

	@Override
	protected Map<String, String> multiOutputOnCalculate(
			Map<String, String> functionData, ShellContext context)
			throws ETLException {
		// TODO Auto-generated method stub
		return null;
	}

}

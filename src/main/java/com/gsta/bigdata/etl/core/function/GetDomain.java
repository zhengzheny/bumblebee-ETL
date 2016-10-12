package com.gsta.bigdata.etl.core.function;

import java.util.Map;

import org.w3c.dom.Element;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.etl.core.ShellContext;
import com.gsta.bigdata.utils.StringUtils;

public class GetDomain extends AbstractFunction {
	private static final long serialVersionUID = 5091858537866550109L;
	@JsonProperty
	private String host;

	public GetDomain() {
		super();
	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		super.initAttrs(element);

		this.host = super.getAttr(Constants.ATTR_HOST);
	}

	@Override
	protected String onCalculate(Map<String, String> functionData,
			ShellContext context) throws ETLException {
		if(functionData == null){
			return null;
		}
		
		String vHost = functionData.get(this.host);
		if (vHost == null || "".equals(vHost)) {
			return null;
		}

		int indexDot = vHost.lastIndexOf(".");
		if (indexDot < 0) {
			return vHost;
		}

		if (StringUtils.isIPV4(vHost)) {
			return vHost;
		}

		String subStr = vHost.substring(0, indexDot);
		indexDot = subStr.lastIndexOf(".", indexDot);
		if (indexDot < 0) {
			return vHost;
		}

		String domain = vHost.substring(indexDot + 1);
		if (domain.startsWith("com") || domain.startsWith("org")
				|| domain.startsWith("gov") || domain.startsWith("edu")
				|| domain.startsWith("net")) {
			subStr = subStr.substring(0, indexDot);
			indexDot = subStr.lastIndexOf(".", indexDot);
			if (indexDot < 0) {
				return domain;
			}
			domain = vHost.substring(indexDot + 1);
		}
		return domain;
	}

	@Override
	protected Map<String, String> multiOutputOnCalculate(
			Map<String, String> functionData, ShellContext context)
			throws ETLException {
		return null;
	}
}

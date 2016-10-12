package com.gsta.bigdata.etl.core.function;

import java.util.Map;

import org.w3c.dom.Element;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.etl.core.ShellContext;
import com.gsta.bigdata.utils.StringUtils;
import com.gsta.bigdata.utils.UrlDecode;

public class GetTaoBao extends AbstractFunction {
	private static final long serialVersionUID = -2542130414818539702L;
	@JsonProperty
	private String cookie;
	@JsonProperty
	private String host;
	private final UrlDecode urlDecode = new UrlDecode();
	
	public GetTaoBao() {
		super();
	}
	
	@Override
	protected void initAttrs(Element element) throws ParseException {
 		super.initAttrs(element);
 		
 		this.cookie = super.getAttr(Constants.ATTR_COOKIE);
 		this.host = super.getAttr(Constants.ATTR_HOST);
	}

	@Override
	protected String onCalculate(Map<String, String> functionData,
			ShellContext context) throws ETLException {
		if(functionData == null){
			return null;
		}
		
		//nickname:"%5Cu7D2B%5Cu817E%5Cu604Bgirl";
		//"hc20cl";
		//"%5Cu5468%5Cu6C0F%5Cu5B9E%5Cu4E1A%5Cu5E97";  		
		String vCookie = functionData.get(this.cookie);
		String vHost = functionData.get(this.host);
		
		if (vHost != null && vCookie != null && vHost.contains("taobao.com")) {
			int idx = vCookie.indexOf("tracknick=");
			if (idx > 0) {
				String taobaoNick = vCookie.substring(idx);
				if("".equals(taobaoNick)) return null;
				// if o_cookie is in middle of cookie field,or o_cookie is the end of cookie field
				idx = taobaoNick.indexOf(";");
				if (idx > 0) {
					taobaoNick = taobaoNick.substring(0, idx);
				}
				taobaoNick = taobaoNick.substring(taobaoNick.indexOf("=") + 1);
				taobaoNick = this.urlDecode.decode(taobaoNick);
				taobaoNick = StringUtils.unicode2ascii(taobaoNick);
				
				return taobaoNick;
			}
		}

		return null;
	}

	@Override
	protected Map<String, String> multiOutputOnCalculate(
			Map<String, String> functionData, ShellContext context)
			throws ETLException {
		return null;
	}
}

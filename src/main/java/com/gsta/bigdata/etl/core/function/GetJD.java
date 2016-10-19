package com.gsta.bigdata.etl.core.function;

import java.util.Map;

import org.w3c.dom.Element;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.etl.core.ShellContext;

public class GetJD extends AbstractFunction {
	private static final long serialVersionUID = -2542130414818539702L;
	@JsonProperty
	private String cookie;
	@JsonProperty
	private String host;
	
	public GetJD() {
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
		
		//visitkey=54931039743732204; webp=1; wq_unionid=; TrackID=UT8XwmJdueYkI7zmnJYDt-ZOWxz4sWCFMB7CUoxz9r16yKY3SZ7m9D917TVyPMFyLWZcSDNjM1ndS4ajn04GilbEu2BoXE8EvCTO373XaxhAkFdAMMYJrBs_17nrSmGS2ckwb33LWugGZMLWcotZgA; string123=8756B6EF9068B1A9C99711868455B29A273T; addrId_1=138251365; addrType_1=1; mt_subsite=122%252C1468703789%7C%7C; __tra=122270672.372214719.1468703789.1468703789.1468703833.1; jdAddrId=; jdAddrName=; buy_uin=4655352860; jdpin=wdPVrdMFNcFuDG; nickname_base64=5buW; open_id=oTGnpnGrQC9OXuvMf79S-gOypVho; openid2=BD8D1DA59434B9681D458DA7355B93E0827F8E6FCF94C8FFDCD8DCB7D5F74A0AE8AEA69BCBDAB48271FE83A6F6EFAF65; picture_url=http%3A%2F%2Fwx.qlogo.cn%2Fmmopen%2FGibvHudxmlJbKibUyWfT1gSKZGZvEBa7e9uf0wGm8zZm1OSvicKAGTicFWlzWpOf4cBQ2eic6N0WFVM
		String vCookie = functionData.get(this.cookie);
		// host:sale.jd.com
		String vHost = functionData.get(this.host);
		
		if (vHost != null && vCookie != null && vHost.contains("jd.com")) {
			int idx = vCookie.indexOf("buy_uin=");
			if (idx > 0) {
				String qq = vCookie.substring(idx);
				// if o_cookie is in middle of cookie field,or o_cookie is the
				// end of cookie field
				idx = qq.indexOf(";");
				if (idx > 0) qq = qq.substring(0, idx);
				idx = qq.indexOf("=");
				if (idx > 0) {
					return qq.substring(idx + 1);
				}
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

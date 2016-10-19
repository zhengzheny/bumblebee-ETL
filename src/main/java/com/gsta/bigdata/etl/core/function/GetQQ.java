package com.gsta.bigdata.etl.core.function;

import java.util.Map;

import org.w3c.dom.Element;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.etl.core.ShellContext;

public class GetQQ extends AbstractFunction {
	private static final long serialVersionUID = -2542130414818539702L;
	@JsonProperty
	private String cookie;
	@JsonProperty
	private String host;
	
	public GetQQ() {
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
		
		//cookie:pgv_flv=19.0 r0; RK=HFtGJtnjRx; __v3_c_review_10685=0; __v3_c_last_10685=1463740094019; __v3_c_visitor=1459230245889309; eas_sid=k174l644Y3d0L7c4505669P8y2; pgv_pvi=7837782016; tvfe_boss_uuid=112e35ccea6290e8; pac_uid=1_718027414; o_cookie=718027414; pt2gguin=o1182316392; uin=o1182316392; skey=@KqjCs8dr8; ptisp=ctc; qzone_check=1182316392_1476146201; ptcz=7f4ea7b42811672309612f4f3589042958138e59a74a633a358d521db63c1e6a; pgv_pvid=4602616853; pgv_info=ssid=s7819139930&pgvReferrer=
		//cuid=4566015531; pac_uid=1_1208455516; tvfe_boss_uuid=c9417acb7c983b18; ptui_loginuin=1208455516; RK=C+MiUcwTF9; ptisp=ctc; ptcz=a85cdf763a548c3ff377b14bd82acf0d4038fa8b48740a44474fc38624dbf777; pt2gguin=o1208455516; uin=o1208455516; skey=@0KRwjH7CM; qzone_check=1208455516_1476147416; uid=128856685; dc_vplaying=0; pgv_info=ssid=s4843552535; pgv_pvid=6552081717; o_cookie=1208455516
		String vCookie = functionData.get(this.cookie);
		// host:isdspeed.qq.com
		String vHost = functionData.get(this.host);
		if (vHost != null && vCookie != null && vHost.contains("qq.com")) {
			int idx = vCookie.indexOf("o_cookie=");
			if (idx > 0) {
				String qq = vCookie.substring(idx);
				if ("".equals(qq)) return null;

				// if o_cookie is in middle of cookie field,or o_cookie is the end of cookie field
				idx = qq.indexOf(";");
				if (idx > 0) {
					qq = qq.substring(0, idx);
				}
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

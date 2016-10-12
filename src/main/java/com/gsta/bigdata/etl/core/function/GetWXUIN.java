package com.gsta.bigdata.etl.core.function;

import java.util.Map;
import org.w3c.dom.Element;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.etl.core.ShellContext;

public class GetWXUIN extends AbstractFunction {
	private static final long serialVersionUID = 1L;
	@JsonProperty
 	private String referer;
	@JsonProperty
	private String cookie;
	@JsonProperty
	private String host;
	

	public GetWXUIN() {
		super();
	}
	
	@Override
	protected void initAttrs(Element element) throws ParseException {
 		super.initAttrs(element);
 		
 		this.referer = super.getAttr(Constants.ATTR_REFER);
 		this.cookie = super.getAttr(Constants.ATTR_COOKIE);
 		this.host = super.getAttr(Constants.ATTR_HOST);
	}

	@Override
	protected String onCalculate(Map<String, String> functionData,
			ShellContext context) throws ETLException {
		if(functionData == null){
			return null;
		}
		
		//get uin from referer
		//http://weixin.qq.com/?version=369302288&uin=1511544840&nettype=1&scene=timeline
		String vReferer = functionData.get(this.referer);
		if(vReferer != null && vReferer.startsWith("http://weixin.qq.com/")){
			int idx = vReferer.indexOf("uin=");
			if(idx > 0){
				String uin = vReferer.substring(idx);
				if("".equals(uin)) return null;
				idx = uin.indexOf("&");
				uin = uin.substring(0, idx);
				uin = uin.substring(uin.indexOf("=") +1);
				
				return uin;
			}
		}
		
		//get uin from cookie
		//host=res.wx.qq.com
		//cookie=pac_uid=1_836946204; ptui_loginuin=2522508286; sd_userid=6651474854752109; sd_cookie_crttime=1474854752109; RK=wDeuZyTyFM; pgv_pvi=1612906496; eas_sid=f1c4X7L4c9u4y8e9z3r9e2W6S9; gaduid=57e9eff95a2c0; rankv=2016091217; pgv_flv=21.9 r9; cuid=4262216420; tvfe_boss_uuid=5d118b482db9401a; uid=415122523; o_cookie=836946204; pgv_si=s5525236736; pgv_info=ssid=s4057571196; pgv_pvid=8363629179; ptisp=cnc; ptcz=5adcc04452af44263496afd37a6e23d048d6195dbcaab6f5c19088c6eb3f532d; pt2gguin=o0836946204; uin=o0836946204; skey=@yyzqb1N36; qzone_check=836946204_1475118293
		String vCookie = functionData.get(this.cookie);
		String vHost = functionData.get(this.host);
		if (vCookie != null && vHost != null && vHost.contains("wx.qq.com")) {
			int idx = vCookie.indexOf("ptui_loginuin=");
			if (idx > 0) {
				String uin = vCookie.substring(idx);
				if("".equals(uin)) return null;
				idx = uin.indexOf(";");
				// if ptui_loginuin is in middle of cookie field,or is the end
				// of cookie field
				if (idx > 0) {
					uin = uin.substring(0, idx);
				}
				uin = uin.substring(uin.indexOf("=") + 1);

				return uin;
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

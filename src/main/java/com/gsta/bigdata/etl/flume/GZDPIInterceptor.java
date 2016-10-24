package com.gsta.bigdata.etl.flume;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

public class GZDPIInterceptor implements Interceptor {
	private String delimiter;
	private String fields;
	private String headerFields;
	private String[] lstFields;
	private String[] lstHeaderFields;
	private static final String NotSeeCharDefineInConf = "001";
	
	public GZDPIInterceptor(String delimiter, String fields, String headerFields) {
		super();
		this.delimiter = delimiter;
		this.fields = fields;
		this.headerFields = headerFields;
	}

	@Override
	public void initialize() {
		if (this.fields != null) {
			this.lstFields = this.fields.split(",", -1);
		}
		
		if(this.headerFields != null){
			this.lstHeaderFields = this.headerFields.split(",",-1);
		}
		
		if(NotSeeCharDefineInConf.equals(this.delimiter)){
			this.delimiter = "\001";
		}
	}

	@Override
	public Event intercept(Event event) {
		if (event == null) {
			return null;
		}

		String line = new String(event.getBody());
		if (line == null || "".equals(line)) {
			return null;
		}
		
		String[] fieldValues = line.split(this.delimiter, -1);
		if (fieldValues.length == this.lstFields.length) {
			Map<String, String> data = new HashMap<String, String>();
			for (int i = 0; i < fieldValues.length; i++) {
				data.put(this.lstFields[i], fieldValues[i]);
			}

			for (String header : this.lstHeaderFields) {
				event.getHeaders().put(header, data.get(header));
			}
		}

		return event;
	}

	@Override
	public List<Event> intercept(List<Event> events) {
		List<Event> retEvents = new ArrayList<Event>();

		for (Event event : events) {
			Event interceptedEvent = intercept(event);  
            if (interceptedEvent != null) {  
            	retEvents.add(interceptedEvent);  
            }  
		}

		return retEvents;
	}

	@Override
	public void close() {
		
	}
	
	public static class Builder implements Interceptor.Builder {
		private String delimiter;
		private String fields;
		private String headerFields;
		
		@Override
		public void configure(Context context) {
			this.delimiter = context.getString("delimiter");
			this.fields = context.getString("fields");
			this.headerFields = context.getString("headerFields");
		}

		@Override
		public Interceptor build() {
			return new GZDPIInterceptor(this.delimiter, this.fields, this.headerFields);
		}  
	}
	
	public static void main(String[] args){
		String s = "1476146415.858109|020067@163.gd|113.99.5.33|12297|119.147.193.12|80|/cgi-bin/v.cgi?flag1=160543&flag2=1&flag3=0&1=100|isdspeed.qq.com|/cgi-bin/v.cgi|flag1=160543&flag2=1&flag3=0&1=100|qq.com|http://appimg2.qq.com/card/index_v3.html|Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36|pgv_pvi=6433741824; RK=cKHyB9NjEo; o_cookie=42939111; pgv_pvid=1808950446; qzone_check=404566092_1476145811; pgv_info=ssid=s1993602880; pgv_si=s116016128; ptui_loginuin=398301179; ptisp=ctc; ptcz=9b3fcdb80b103bbfa8c779671298aaedbc0f4478675ee5301c95cd236d9da555; pt2gguin=o0398301179; uin=o0398301179; skey=@l5SOkjwIa; qqmusic_uin=; qqmusic_key=; qqmusic_fromtag=|null|42939111|null|null|null|null|null|8|20161011084015.858109|20161011|host117";
		String fields = "ts,ad,srcip,srcport,dstip,dstport,url,host,path,query,domain,ref,ua,cookie,weixinid,qq,buy_uin,taobao_nick,weibosup,weiboname,weibonick,hour,timeStamp,day,collectHost";
		String header = "day,hour";
		GZDPIInterceptor i = new GZDPIInterceptor("\\|",fields,header);
		i.initialize();
		Event event = new org.apache.flume.event.SimpleEvent();
		event.setBody(s.getBytes());
		i.intercept(event);
		System.out.println(event);
	}
}

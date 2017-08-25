package com.gsta.bigdata.etl.flume;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import com.gsta.bigdata.etl.flume.sources.SpoolDirectorySourceConstants;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.gsta.bigdata.etl.flume.sources.SpoolDirectoryCompressSource;

public class DPIInterceptor implements Interceptor {
	private String delimiter;
	private String headerName;
	private int[] fields;
	private int keyField;
	private int kafkaPartitions = 256;
	private static final String NotSeeCharDefineInConf = "001";
	private String outputDelimiter;
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
	final Logger logger = LoggerFactory.getLogger(this.getClass());
	private Random random = new Random();
	public static Map<String,String> MapDomain = new HashMap<String,String>();
	private static boolean  domainFlag = true;


	public DPIInterceptor(String delimiter, int[] fields, int keyField,
						  String headerName,int kafkaPartitions,String fileNameSimpleDateFormat) {
		super();
		this.delimiter = delimiter;
		this.fields = fields;
		this.headerName = headerName;
		this.keyField = keyField;
		this.kafkaPartitions = kafkaPartitions;

		this.outputDelimiter = StringEscapeUtils.unescapeJava(this.delimiter);
		if (NotSeeCharDefineInConf.equals(this.delimiter)) {
			this.delimiter = "\001";
		}
		if(fileNameSimpleDateFormat != null){
			this.sdf = new SimpleDateFormat(fileNameSimpleDateFormat);
		}
	}

	@Override
	public void initialize() {

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
		if (fieldValues == null) {
			return null;
		}

		StringBuffer sb = new StringBuffer();
		boolean flag = false;
		String domain = "";
		for (int i : this.fields) {
			if (i > 0 && i <= fieldValues.length) {
				if(i==18){
					domain = MapDomain.get(fieldValues[i - 1]);
					if (domain == null){
						domain = "其他";
					}
					sb.append(domain).append(this.outputDelimiter);
				}else{
					sb.append(fieldValues[i - 1]).append(this.outputDelimiter);
				}
				flag = true;
			}else{
				sb.append("").append(this.outputDelimiter);
			}
		}
		if (flag) {
			line = sb.toString().substring(0,sb.length() - this.outputDelimiter.length());
		}

		//get timestamp from file name
		//f_8_S1udns-Guangdong-20161107190754.txt.gz
		//f_10_S1uhttp-Guangdong-20161105160957.txt.gz
		//f_11_S1uother-Guangdong-20161103031029.txt.gz
		//S1ustraming-Guangdong-20161024100021.txt.gz
		String fileName = event.getHeaders().get(SpoolDirectorySourceConstants.DEFAULT_BASENAME_HEADER_KEY);
		if(fileName == null){
			//default is system time
			line = line + this.outputDelimiter + System.currentTimeMillis();
		}else{
			int idx1 = fileName.lastIndexOf("-");
			int idx2 = fileName.indexOf(".");
			if(idx1 >0 && idx2 >0){
				String ts = fileName.substring(idx1+1, idx2);
				//由于数据源文件名来回变,有时候是20170330111133,有时候是2017033010394601,统一取前14位
				if(ts.length() >=14)  ts = ts.substring(0, 14);
				try {
					line = line + this.outputDelimiter + this.sdf.parse(ts).getTime();
				} catch (ParseException e) {
					logger.error("parse filename=" + fileName + ",occur " + e.getMessage());
				}
			}else{
				line = line + this.outputDelimiter + System.currentTimeMillis();
			}
		}

		event.setBody(line.getBytes());
		if (this.keyField > 0 && this.keyField <= fieldValues.length) {
			String keyValue = fieldValues[this.keyField - 1];
			//if invalid mdn,set random
			if(keyValue == null || keyValue.length() < 6){
				keyValue = String.valueOf(random.nextInt(this.kafkaPartitions));
			}
			event.getHeaders().put(this.headerName,keyValue);
		}

		return event;
	}

	@Override
	public List<Event> intercept(List<Event> events) {
		List<Event> retEvents = new ArrayList<Event>();
		if(domainFlag){
			ReadSource();
			logger.info("mapdoamin size:"+MapDomain.size());
			domainFlag= false;
		}
		for (Event event : events) {
			Event interceptedEvent = intercept(event);
			if (interceptedEvent != null) {
				retEvents.add(interceptedEvent);
			}
		}

		return retEvents;
	}
	public static void ReadSource(){
		String  lineStr ="";
		try{
			FileInputStream fis = new FileInputStream("conf/Newdomain.txt");
			InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
			BufferedReader bufReader = new BufferedReader(isr);
			while(true)
			{
				lineStr = bufReader.readLine();
				if(lineStr != null)
				{
					String[] domain = lineStr.split("\\|");
					MapDomain.put(domain[0], domain[1]);
				}
				else
				{
					break;
				}
			}
			bufReader.close();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}
	@Override
	public void close() {

	}

	public static class Builder implements Interceptor.Builder {
		private String delimiter;
		private String strFields;
		private String headerName;
		private String strKeyField;
		private int[] fields;
		private int keyField;
		private int kafkaPartitions = 256;
		private String fileNameSimpleDateFormat;

		@Override
		public void configure(Context context) {
			this.delimiter = context.getString("delimiter");
			this.strFields = context.getString("fields");
			this.headerName = context.getString("headerName");
			this.strKeyField = context.getString("keyField");

			if (this.strFields != null) {
				String[] strs = this.strFields.split(",", -1);
				this.fields = new int[strs.length];
				for (int i = 0; i < strs.length; i++) {
					this.fields[i] = Integer.parseInt(strs[i]);
				}
			}

			if (this.strKeyField != null) {
				this.keyField = Integer.parseInt(this.strKeyField);
			}

			this.kafkaPartitions = Integer.parseInt(context.getString("kafkaPartitions"));
			this.fileNameSimpleDateFormat = context.getString("fileNameSimpleDateFormat");
			if(this.fileNameSimpleDateFormat == null){
				this.fileNameSimpleDateFormat = "yyyyMMddHHmmss";
			}
		}


		@Override
		public Interceptor build() {
			return new DPIInterceptor(this.delimiter, this.fields,
					this.keyField, this.headerName,this.kafkaPartitions,this.fileNameSimpleDateFormat);
		}
	}

	public static void main(String[] args) {
		String s = "1|4601104310583|8618125640|8679310927614|CTNET|163.177.81.139|80|100.85.92.123|39915|8.128.0.205|115.169.194.37|115.169.132.149|4601186B4930|460117A5C|46011|6|18|205|20161024095948|20161024095948|0|0|748|0|1|20161023014337|5|5008000000000000000000000|1|8.142.65.45|2152|2152|1286736896|61140717||||||||100.85.92.123|39915|0|163.177.81.139|80|1|0|0|0|0|0|0|0|0|0|0|0|0|0|1|MicroMessenger Client|szextshort.weixin.qq.com/mmtls/32be6ba0|szextshort.weixin.qq.com|szextshort.weixin.qq.com|474|application/octet-stream|0||5|-2|0|0|1477274388418|1477274388418||1477274388418|1477274388418||||3||7235434166285||||||99|";
		Context ctx = new Context();
		ctx.put("delimiter", "\\|");
		ctx.put("fields", "2,3,10,13");
		ctx.put("headerName", "key");
		ctx.put("keyField", "3");

		DPIInterceptor.Builder builder = new DPIInterceptor.Builder();
		builder.configure(ctx);

		DPIInterceptor i = (DPIInterceptor)builder.build();
		i.initialize();
		Event event = new org.apache.flume.event.SimpleEvent();
		event.getHeaders().put("key", "f_8_S1udns-Guangdong-20161107190754.txt.gz");
		event.setBody(s.getBytes());
		i.intercept(event);
		System.out.println(new String(event.getBody()));
		System.out.println(event.getHeaders());
	}
}

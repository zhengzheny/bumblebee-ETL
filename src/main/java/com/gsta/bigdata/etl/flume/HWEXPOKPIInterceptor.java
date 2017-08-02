package com.gsta.bigdata.etl.flume;

import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.ETLProcess;
import com.gsta.bigdata.etl.flume.sources.SpoolDirectorySourceConstants;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HWEXPOKPIInterceptor extends AbstractKPIInterceptor {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private Map<String, ETLData> mapEtl = new HashMap<>();

    public HWEXPOKPIInterceptor(String configFilePath, String[] types) {
        super(configFilePath, types);
    }

    @Override
    protected String getConfigFileByType(String type) {
        return super.configFilePath + "ETL_EXPO_KPI_HW_" + type + ".xml";
    }

    @Override
    protected String getTypeFromSource(String fileName) {
        //PM201612220330+0800_20161222.0300+0800-0315+0800_CELLPOWER.xml
        //return CELLPOWER
//		if (fileName != null) {
//			int pos = fileName.lastIndexOf("_");
//			if (pos > 0) {
//				String key = fileName.substring(pos + 1);
//				pos = key.lastIndexOf(".");
//				if (pos > 0)
//					return key.substring(0, pos);
//			}
//		}

        return "ALL";
    }

    @Override
    protected Map<String, String> parseHeadersBySource(String fileName) {
        // PM201612220330+0800_20161222.0300+0800-0315+0800_CELLPOWER.xml
        //HEADER_KPI_DATE=20161222
        //HEADER_KPI_HOUR=0300
        Map<String, String> headers = new HashMap<String, String>();

        boolean flag = false;
        String d = fileName.substring(6, 14);
        String h = fileName.substring(15, 19);
        headers.put(HEADER_KPI_DATE, d+h);
//        headers.put(HEADER_KPI_HOUR, h);
        flag = true;
        if (!flag) {
            headers.put(HEADER_KPI_DATE, HEADER_DEFAULT_VALUE);
//            headers.put(HEADER_KPI_HOUR, HEADER_DEFAULT_VALUE);
        }
        return headers;
    }

    public static class Builder extends AbstractKPIBuilder {
        @Override
        public Interceptor build() {
            return new HWEXPOKPIInterceptor(super.configFilePath, super.types);
        }
    }

    @Override
    public Event intercept(Event event) {
        if (event == null) {
            return null;
        }

        String fileName = event.getHeaders().get(
                SpoolDirectorySourceConstants.DEFAULT_BASENAME_HEADER_KEY);
        String type = this.getTypeFromSource(fileName);
        ETLProcess process = this.processes.get(type);
        if (process == null) {
            logger.error(fileName + " get null process");
            return null;
        }

        String line = new String(event.getBody());
        String output="";
        String sb="";
       try {
            List<ETLData> datalist = process.parseLine(line);
            if (datalist != null) {
            System.out.println("list size" + datalist.size());
                for (ETLData data : datalist) {
                    if (data != null) {
                        process.onTransform(data);
                        output = process.getOutputValue(data);
                       System.out.println(output);
                        sb=sb+output+"\n";
                    }
                }
                sb=sb.substring(0,sb.length() - 1);
                if (sb != null) {
                    event.setBody(sb.getBytes());
                }
                Map<String, String> headers = event.getHeaders();
                headers.put(HEADER_TYPE, type);
                headers.putAll(this.parseHeadersBySource(fileName));
                return event;

            }
        } catch (Exception e) {
            logger.error("dataline=" + line + ",error:" + e.getMessage());
        }
        return null;
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


	public static void main(String[] args) {
		String fileName = "A20170630.1500+0800-1600+0800_caF澳岐大厦LTE-BBU01.xml";
//		HWEXPOKPIInterceptor kpi = new HWEXPOKPIInterceptor(null,null);
        String d = fileName.substring(1, 9);
        String h = fileName.substring(10, 14);
        System.out.println(d+h);
//		System.out.println(kpi.getTypeFromSource(fileName));
//		System.out.println(kpi.parseHeadersBySource(fileName));
	}
}

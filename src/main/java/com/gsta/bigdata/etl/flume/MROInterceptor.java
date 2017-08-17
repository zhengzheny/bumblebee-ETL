package com.gsta.bigdata.etl.flume;

import com.gsta.bigdata.etl.ETLRunner;
import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.ETLProcess;
import com.gsta.bigdata.etl.flume.sources.SpoolDirectorySourceConstants;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by tianxq on 2017/7/12.
 */
public class MROInterceptor implements Interceptor {
    private  String configFile;
    //etl处理流程，由配置文件进行初始化
    private ETLProcess etlProcess;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    //protected final static String HEADER_TYPE = "type";
    protected final static String HEADER_KPI_DATE = "kd";
    protected final static String HEADER_KPI_HOUR = "kh";
    protected final static String HEADER_DEFAULT_VALUE = "NULL";

    public MROInterceptor(String configFile){
        this.configFile = configFile;
    }
    @Override
    public void initialize() {
        if(this.configFile != null) {
            //根据配置文件初始化etl处理流程
            Element processNode = new ETLRunner().getProcessNode(this.configFile, null);
            if (processNode == null) {
                throw new RuntimeException(this.configFile + " get null process node...");
            }

            this.etlProcess = new ETLProcess();
            this.etlProcess.init(processNode);
        }
    }

    @Override
    //处理文件中的单条记录
    public Event intercept(Event event) {
        if (event == null)  return null;

        String fileName = event.getHeaders().get(
                SpoolDirectorySourceConstants.DEFAULT_BASENAME_HEADER_KEY);
        //String type = getTypeFromSource(fileName);

        if(this.etlProcess == null){
            logger.error("etl process object is null");
            return null;
        }

        String line = new String(event.getBody());
        try {
            //实际上由配置文件etl-mro-zte.xml的sourceMetaData中的type去解析，得到k-v记录
            ETLData data = this.etlProcess.parseLine(line, null);
            if (data != null) {
                //进行转换函数处理
                this.etlProcess.onTransform(data);
                //按照配置文件中的输出字段进行格式化输出
                String output = this.etlProcess.getOutputValue(data);

                if (output != null) {
                    event.setBody(output.getBytes());
                    Map<String, String> headers = event.getHeaders();
                    //headers.put(HEADER_TYPE, type);
                    headers.putAll(parseHeadersBySource(fileName));
                    return event;
                }
            }
        }catch(Exception e){
            logger.error("dataline=" + line + ",error:" + e.getMessage());
        }

        return null;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        //这个方法，flume主线程会调用，一次批量处理，无需修改
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

    /*protected String getTypeFromSource(String fileName) {
        String type;
        if(fileName.contains("MRO")) {
            type = "MRO";
            return type;
        } else if (fileName.contains("MRE")) {
            type = "MRE";
            return type;
        }else if (fileName.contains("MRS")){
            type = "MRS";
            return type;
        }else{
            return null;
        }
    }*/

    protected Map<String, String> parseHeadersBySource(String fileName) {
        //pmresult_1526726660_15_201612220300_201612220315.xml
        //HEADER_KPI_DATE=20161222
        //HEADER_KPI_HOUR=0300
        boolean flag = false;
        Map<String, String> headers = new HashMap<String, String>();
        if(fileName != null){
            int pos1 = fileName.lastIndexOf("_");
            int pos2 = fileName.indexOf(".");
            String p = fileName.substring(pos1 + 1, pos2);
            if(p.length() >= 14){
                String d = p.substring(0, 8);
                String h = p.substring(0, 10);
                headers.put(HEADER_KPI_DATE, d);
                headers.put(HEADER_KPI_HOUR, h);
                flag = true;
            }
        }

        if(!flag){
            headers.put(HEADER_KPI_DATE, HEADER_DEFAULT_VALUE);
            headers.put(HEADER_KPI_HOUR, HEADER_DEFAULT_VALUE);
        }
        return headers;
    }

    public static class Builder implements Interceptor.Builder {
        //ETL配置文件路径
        private  String configFile;

        @Override
        public Interceptor build() {
            return new MROInterceptor(this.configFile);
        }

        @Override
        public void configure(Context context) {
            //获取flume-mrotest.conf配置文件的值，并传入解析器
            // etlAgent.sources.src1.interceptors.etlInterceptor.configFilePath
            this.configFile = context.getString("configFile");
        }
    }
}

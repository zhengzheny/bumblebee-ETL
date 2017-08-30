package com.gsta.bigdata.etl.flume;

import com.gsta.bigdata.etl.ETLRunner;
import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.ETLProcess;
import com.gsta.bigdata.etl.core.source.mronew.MroXmlParser;
import com.gsta.bigdata.etl.flume.sources.SpoolDirectorySourceConstants;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Created by tianxq on 2017/7/12.
 */
public class MROInterceptor implements Interceptor {
    private String configFile;
    private  int partitionCount;
    //etl处理流程，由配置文件进行初始化
    private ETLProcess etlProcess;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    public MROInterceptor(String configFile,int partitionCount) {
        this.configFile = configFile;
        this.partitionCount = partitionCount;
    }

    @Override
    public void initialize() {
        if (this.configFile != null) {
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
        if (event == null) return null;

        if (this.etlProcess == null) {
            logger.error("etl process object is null");
            return null;
        }

        String line = new String(event.getBody());
        try {
            List<ETLData> etlDataList = this.etlProcess.parseLine(line);
            StringBuffer sb = new StringBuffer();
            for (ETLData data : etlDataList) {
                //进行转换函数处理
                this.etlProcess.onTransform(data);
                //按照配置文件中的输出字段进行格式化输出
                String output = this.etlProcess.getOutputValue(data);
                sb.append(output).append("\n");

            }
            if (sb.length() > 0) {
                event.setBody(sb.toString().getBytes());

                //一个eNB,作为多条记录发送出去，但enodeid和开始时间一样，用来做分拣
                String enodeid = etlDataList.get(0).getValue(MroXmlParser.ENODEID);
                String startTime = etlDataList.get(0).getValue(MroXmlParser.STARTTIME);
                String[] t = getPartitionTime(startTime);
                String ppid = String.valueOf(getProcessID());
                String ip = getLastIp();
                Map<String, String> headers = event.getHeaders();
                headers.put("pid",getPartitionId(enodeid));
                headers.put("day",t[0]);
                headers.put("hour",t[1]);
                headers.put("ip",ip);
                headers.put("ppid",ppid);

                return event;
            }
        } catch (Exception e) {
          //  logger.error("dataline=" + line + ",error:" + e.getMessage());
        }

        return null;
    }

    private int getProcessID() {
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();

        return Integer.valueOf(runtimeMXBean.getName().split("@")[0]).intValue();
    }

    private String getLastIp(){
        try {
            InetAddress addr = InetAddress.getLocalHost();
            String ip = addr.getHostAddress().toString();
            ip = ip.substring(ip.lastIndexOf(".") + 1, ip.length());
            return ip;
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        return "-1";
    }

    private String[] getPartitionTime(String startTime){
        String[] ret = new String[2];

        String dateStr = startTime;
        if(startTime == null || "".equals(startTime) || startTime.length() < 13){
            Date currentTime = new Date();
            dateStr = MroXmlParser.df_a.format(currentTime);
        }

        //2017-08-07 04:15:00.000
        ret[0] = dateStr.substring(0,4) + dateStr.substring(5,7) + dateStr.substring(8,10);
        ret[1] = ret[0] + dateStr.substring(11,13);
        return ret;
    }

    private String getPartitionId(String enodeid) {
        int ret = partitionCount;
        if (enodeid != null && !"".equals(enodeid)) {
            try {
                int enbid = Integer.parseInt(enodeid);
                if (enbid > 0) {
                    ret = enbid % partitionCount;
                }
            } catch (NumberFormatException e) {  }
        }

        return String.valueOf(ret);
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

    public static class Builder implements Interceptor.Builder {
        //ETL配置文件路径
        private String configFile;
        private  int partitionCount;

        @Override
        public Interceptor build() {
            return new MROInterceptor(configFile,partitionCount);
        }

        @Override
        public void configure(Context context) {
            //获取flume-mrotest.conf配置文件的值，并传入解析器
            // etlAgent.sources.src1.interceptors.etlInterceptor.configFilePath
            this.configFile = context.getString("configFile");
            this.partitionCount = context.getInteger("partitionCount");
        }
    }
}

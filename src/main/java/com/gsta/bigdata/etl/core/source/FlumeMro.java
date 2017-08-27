package com.gsta.bigdata.etl.core.source;

import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.ETLRunner;
import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.ETLProcess;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.etl.core.source.mronew.ERSMroXmlParser;
import com.gsta.bigdata.etl.core.source.mronew.MroXmlParser;
import com.gsta.bigdata.etl.core.source.mronew.HWMroXmlParser;
import com.gsta.bigdata.etl.core.source.mronew.ZTEMroXmlParser;
import org.w3c.dom.Element;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Created by tianxq on 2017/8/23.
 */
public class FlumeMro extends AbstractSourceMetaData{
    private static final long serialVersionUID = 1L;
    private MroXmlParser parser;

    public FlumeMro() {
        super();
    }

    @Override
    protected void initAttrs(Element element) throws ParseException {
        super.initAttrs(element);

        String type = super.getAttr("parseType");
        if (type == null || "".equals(type)){
            throw  new ParseException("has no parseType");
        }

        switch (type){
            case "HW":
               parser = new HWMroXmlParser();
                break;
            case "ZTE":
                parser = new ZTEMroXmlParser();
                break;
            case "ERS":
                parser = new ERSMroXmlParser();
                break;
            default:
                parser = new HWMroXmlParser();
        }
    }

    @Override
    public ETLData parseLine(String line, Set<String> invalidRecords) throws ETLException, ValidatorException {
        return null;
    }

    @Override
    public List<ETLData> parseLine(String line) throws ETLException, ValidatorException {
        if(parser != null){
            return parser.parseLine(line);
        }

        return null;
    }

    public static void main(String[] args) {
        String configFile = "./flume/mro/flume-etl-mro-hw.xml";
        Element processNode = new ETLRunner().getProcessNode(configFile, null);
        if (processNode == null) {
            throw new RuntimeException(configFile + " get null process node...");
        }

        ETLProcess etlProcess = new ETLProcess();
        etlProcess.init(processNode);

        String fileName = "D:\\user\\ultrainm\\origindata\\I3\\DATA\\PUBLIC\\NOCE\\SRC\\SRC_4G_MRO_HW\\20170807\\2017080704\\FDD-LTE_MRO_HUAWEI_491299_20170807041500.xml";
        File file = new File(fileName);
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String line = null;
             while ((line = reader.readLine()) != null) {
                 List<ETLData> datas = etlProcess.parseLine(line);
                 for(ETLData data:datas){
                     System.out.println(etlProcess.getOutputValue(data));
                 }
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
    }
}

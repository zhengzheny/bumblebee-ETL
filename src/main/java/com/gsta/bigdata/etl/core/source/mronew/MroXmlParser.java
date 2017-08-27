package com.gsta.bigdata.etl.core.source.mronew;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.source.ValidatorException;
import com.gsta.bigdata.etl.core.source.mronew.MROObject.*;
import com.gsta.bigdata.utils.SourceXmlTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by tianxq on 2017/8/22.
 */
public abstract class MroXmlParser {
    private static final Logger log = LoggerFactory.getLogger(MroXmlParser.class);

    public static final char fieldSeparatorChar = '|';
    public static final String fieldSplitRegex = "\\|";

    public static final  String ENODEID = "ENODEID";
    public static final  String STARTTIME = "startTime";

    protected final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    public static final SimpleDateFormat df_a = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    protected String startTime;
    protected String endTime;

    protected SmrType smrType;

    protected Smr currentSmr;
    protected final Smr1 smr1 = new Smr1();
    protected final Smr2 smr2 = new Smr2();
    protected final Smr3 smr3 = new Smr3();

    protected String enb_id;
    protected List<ETLData>  lstEtlData = new ArrayList<ETLData>();

    /**
     * 根据从xml文件中读取行，并进行解析
     * @param line
     * @return
     * @throws ETLException
     * @throws ValidatorException
     */
    public List<ETLData> parseLine(String line) throws ETLException,ValidatorException {
        if (line == null || "".equals(line)) {
            return null;
        }

        ElementType elementType = getType(line);
        switch (elementType) {
            case bulkPmMrDataFile:
                startTime = null;
                endTime = null;
                break;
            case fileHeader:
                parseFileHeader(line);
                break;
            case eNB:
                //清理已有缓存
                currentSmr = null;
                smr1.clear();
                smr2.clear();
                smr3.clear();

                lstEtlData.clear();

                enb_id = getEnbId(line);
                break;
            case smr:
                parseSMR(line);
                break;
            case object:
                parseObject(line);
                break;
            case v:
                parseV(line);
                break;
            case eNBEnd:
                //</eNB>结束后，把数据发送过去
                createRecords();
                break;
            case bulkPmMrDataFileEnd:
                //清理已有缓存
                currentSmr = null;
                smr1.clear();
                smr2.clear();
                smr3.clear();

                lstEtlData.clear();
                break;
            default:
        }

        return lstEtlData;
    }

    protected abstract void createRecords();

    private void parseV(String line){
        String data = SourceXmlTool.getTagValue(line, "v");
        if(data == null || "".equals(data)) return;

        String[] a = null;
        try {
            char[] ch = data.toCharArray();
            a = splitValue(ch, 0, ch.length);
        } catch (Exception e) {
            log.error("Invalid v: " + data, e);
        }

        if (a != null) {
            switch (smrType) {
                case smr1:
                    smr1.current.values.add(a);
                    break;
                case smr2:
                    smr2.smrObject2s.put(smr2.current, a);
                    break;
                case smr3:
                    smr3.current.value.sum += toFloat(a[0]);
                    smr3.current.value.count++;
                    break;
            }
        }
    }

    protected static float toFloat(String str) {
        if (str == null)
            return 0.0f;
        else {
            try {
                return Float.parseFloat(str);
            } catch (Exception e) {
                return 0.0f;
            }
        }
    }

    /**
     * 把42 41 20 14 100 301 100 195 3 18 NIL 36 NIL 4 NIL NIL 0 NIL NIL NIL NIL NIL NIL NIL NIL
     * 解析成数组，对于NIL去掉
     * @param ch
     * @param start
     * @param length
     * @return
     */
    protected String[] splitValue(char[] ch, int start, int length) {
        int end = start + length;
        while (ch[start] == ' ' && start < end)
            start++;

        while (ch[end - 1] == ' ' && end >= start)
            end--;
        int last = start;
        int fieldCount = currentSmr.fieldCount;
        String[] a = new String[fieldCount];
        int index = 0;
        for (int i = start; i < end; i++) {
            if (ch[i] == ' ') {
                int count = i - last;
                if (count == 3 && ch[last] == 'N' && ch[last + 1] == 'I' && ch[last + 2] == 'L') {
                    a[index++] = null;
                } else {
                    String str = new String(ch, last, count);
                    a[index++] = str;
                }
                last = i + 1;
                if (index > fieldCount) {
                    log.warn("Invalid object value", new String(ch, start, end - start));
                    return null;
                }
            } else if (i == end - 1) {
                int count = end - last;
                if (count == 3 && ch[last] == 'N' && ch[last + 1] == 'I' && ch[last + 2] == 'L') {
                    a[index++] = null;
                } else {
                    String str = new String(ch, last, count);
                    a[index++] = str;
                }
                last = i + 1;
                if (index > fieldCount) {
                    log.warn("Invalid object value", new String(ch, start, end - start));
                    return null;
                }
            }
        }
        return a;
    }

    private void parseObject(String line){
        String timeStamp = SourceXmlTool.getAttrValue(line, "TimeStamp");
        if(timeStamp == null){
            timeStamp = SourceXmlTool.getAttrValue(line, "MR.TimeStamp");
        }

        String id = SourceXmlTool.getAttrValue(line, "id");
        String mrObjId = null;
        if (id == null)
            mrObjId = SourceXmlTool.getAttrValue(line,"MR.objectId");

        String mmeCode = SourceXmlTool.getAttrValue(line, "MmeCode");
        if(mmeCode == null){
            mmeCode = SourceXmlTool.getAttrValue(line, "MR.MmeCode");
        }

        String mmeGroupId = SourceXmlTool.getAttrValue(line,"MmeGroupId");
        if(mmeGroupId == null){
            mmeGroupId = SourceXmlTool.getAttrValue(line,"MR.MmeGroupId");
        }

        String mmeUeS1apId = SourceXmlTool.getAttrValue(line,"MmeUeS1apId");
        if(mmeUeS1apId == null){
            mmeUeS1apId = SourceXmlTool.getAttrValue(line,"MR.MmeUeS1apId");
        }

        switch (smrType) {
            case smr1:
                smr1.current = new SmrObject1();
                smr1.smrObject1s.add(smr1.current);
                smr1.current.timeStamp = timeStamp;
                smr1.current.id = id;
                smr1.current.mrObjId = mrObjId;
                smr1.current.mmeCode = mmeCode;
                smr1.current.mmeGroupId = mmeGroupId;
                smr1.current.mmeUeS1apId = mmeUeS1apId;
                break;
            case smr2:
                smr2.current = new SmrObject2();
                smr2.current.timeStamp = timeStamp;
                smr2.current.id = id;
                smr2.current.mrObjId = mrObjId;
                smr2.current.mmeCode = mmeCode;
                smr2.current.mmeGroupId = mmeGroupId;
                smr2.current.mmeUeS1apId = mmeUeS1apId;
                break;
            case smr3:
                SmrObject3 smrObject3 = new SmrObject3(id, mrObjId, timeStamp);
                SmrObject3Value value = smr3.smrObject3s.get(smrObject3);
                if (value == null) {
                    value = new SmrObject3Value();
                    smr3.smrObject3s.put(smrObject3, value);
                }
                smrObject3.value = value;
                smr3.current = smrObject3;
                break;
        }
    }

    private void parseSMR(String line){
        String smr = SourceXmlTool.getTagValue(line, "smr");
        if(smr == null || "".equals(smr))  return;

        HashMap<String, Integer> fieldMap = new HashMap<>();
        int index = 0;
        for (String f : smr.split(" ")) {
            f = f.trim();
            if (f.isEmpty()) continue;
            fieldMap.put(f, index++);
        }
        currentSmr = null;
        if (fieldMap.containsKey("MR.LteScRSRP")) {
            smrType = SmrType.smr1;
            currentSmr = smr1;
        } else if (fieldMap.containsKey("MR.LteScPlrULQci1")) {
            smrType = SmrType.smr2;
            currentSmr = smr2;
        } else if (fieldMap.containsKey("MR.LteScRIP")) {
            smrType = SmrType.smr3;
            currentSmr = smr3;
        }

        if (currentSmr != null) {
            currentSmr.fieldMap = fieldMap;
            currentSmr.fieldCount = fieldMap.size();
        }
    }

    private String getEnbId(String line){
        String enb_id = SourceXmlTool.getAttrValue(line, "id");
        if (enb_id == null) {
            enb_id = SourceXmlTool.getAttrValue(line, "MR.eNBId");
            if (enb_id == null)
                enb_id = SourceXmlTool.getAttrValue(line, "eNBId");
        }
        return enb_id;
    }

    private void parseFileHeader(String line){
        startTime = SourceXmlTool.getAttrValue(line, "startTime");
        startTime = df_a.format(new Date(toTime(startTime)));
        endTime = SourceXmlTool.getAttrValue(line, "endTime");
        endTime = df_a.format(new Date(toTime(endTime)));
    }

    private ElementType  getType(String line){
        if (line.indexOf("<fileHeader") != -1) {
            return ElementType.fileHeader;
        }else if(line.indexOf("<eNB") != -1){
            return ElementType.eNB;
        }else if(line.indexOf("<smr") != -1){
            return ElementType.smr;
        }else if(line.indexOf("<object") != -1){
            return ElementType.object;
        }else if(line.indexOf("<v") != -1){
            return ElementType.v;
        }else if(line.indexOf("</eNB>") != -1){
            return ElementType.eNBEnd;
        }else if(line.indexOf("<bulkPmMrDataFile>") != -1){
            return ElementType.bulkPmMrDataFile;
        }else if(line.indexOf("</bulkPmMrDataFile>") != -1){
            return ElementType.bulkPmMrDataFileEnd;
        }

        return ElementType.unknow;
    }

    protected long toTime(String str) {
        if (str == null) return -1;
        try {
            return df.parse(str).getTime();
        } catch (Exception e) {
            log.warn("Invalid time: " + str, e);
            return -1;
        }
    }

    protected static int toInt(String str) {
        if (str == null)
            return 0;
        try {
            return Integer.parseInt(str);
        } catch (Exception e) {
            return 0;
        }
    }

    protected int getFieldIndex(String name) {
        Integer index = smr1.fieldMap.get(name);
        if (index == null) {
            if (smr2.fieldMap != null) {
                index = smr2.fieldMap.get(name);
            }
        }
        if (index == null) {
            return -1;
        } else
            return index.intValue();
    }

    protected String v(String[] values, int fieldIndex) {
        if (fieldIndex < 0 || fieldIndex >= values.length)
            return null;
        else
            return values[fieldIndex];
    }

    public static boolean isInvalid(String str) {
        if (str == null)
            return true;
        str = str.trim();
        if (str.isEmpty() || str.equals("0"))
            return true;
        return false;
    }

    public static boolean isNullOrEmpty(String str) {
        if (str == null)
            return true;
        if (str.trim().isEmpty())
            return true;
        return false;
    }

    // cell_id有可能为0的，所以异常情况时返回 -1
    protected static int toCellId(String str) {
        if (str == null)
            return -1;
        try {
            return Integer.parseInt(str);
        } catch (Exception e) {
            return -1;
        }
    }
}

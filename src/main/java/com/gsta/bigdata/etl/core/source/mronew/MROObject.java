package com.gsta.bigdata.etl.core.source.mronew;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by tianxq on 2017/8/23.
 */
public class MROObject {
    public static enum ElementType {
        fileHeader,
        eNB,
        smr,
        object,
        v,
        eNBEnd,
        bulkPmMrDataFile,
        bulkPmMrDataFileEnd,
        unknow;
    }

    //每一个mro由三段measurement的smr组成，但有些只有第一个
    public static enum SmrType {
        smr1,
        smr2,
        smr3;
    }

    //srm对应的object对象
    public static class SmrObject {
        String id;
        String mrObjId;
        String mmeUeS1apId;
        String mmeGroupId;
        String mmeCode;
        String timeStamp;

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((id == null) ? 0 : id.hashCode());
            result = prime * result + ((mmeCode == null) ? 0 : mmeCode.hashCode());
            result = prime * result + ((mmeUeS1apId == null) ? 0 : mmeUeS1apId.hashCode());
            result = prime * result + ((timeStamp == null) ? 0 : timeStamp.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            // 不要加这个，否则子类都不正确
            // if (getClass() != obj.getClass())
            // return false;
            SmrObject other = (SmrObject) obj;
            if (id == null) {
                if (other.id != null)
                    return false;
            } else if (!id.equals(other.id))
                return false;
            if (mmeCode == null) {
                if (other.mmeCode != null)
                    return false;
            } else if (!mmeCode.equals(other.mmeCode))
                return false;
            if (mmeUeS1apId == null) {
                if (other.mmeUeS1apId != null)
                    return false;
            } else if (!mmeUeS1apId.equals(other.mmeUeS1apId))
                return false;
            if (timeStamp == null) {
                if (other.timeStamp != null)
                    return false;
            } else if (!timeStamp.equals(other.timeStamp))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "SmrObject [id=" + id + ", mmeUeS1apId=" + mmeUeS1apId + ", mmeGroupId=" + mmeGroupId + ", mmeCode="
                    + mmeCode + ", timeStamp=" + timeStamp + "]";
        }
    }

    //smr有多少个字段
    public static class Smr {
        int fieldCount;
        //key=字段名称，value=字段序号
        HashMap<String, Integer> fieldMap;

        public void clear(){
            if(fieldMap != null) fieldMap.clear();
        }
    }

    public static class Smr1 extends Smr {
        SmrObject1 current;
        //一个smr中有多少个object
        ArrayList<SmrObject1> smrObject1s = new ArrayList<>();

        @Override
        public void clear() {
            super.clear();
            current = null;
            smrObject1s.clear();
        }
    }

    public static class SmrObject1 extends SmrObject {
        //对应的v行多少个
        ArrayList<String[]> values = new ArrayList<>();
    }

    public static class Smr2 extends Smr {
        SmrObject2 current;
        //key=smrObject，这个和smr1的object进行关联；value=smr2 object对应的v行
        HashMap<SmrObject, String[]> smrObject2s = new HashMap<>();

        @Override
        public void clear() {
            super.clear();
            current = null;
            smrObject2s.clear();
        }
    }

    //TODO：可否干掉
    public static class SmrObject2 extends SmrObject {
        // String[] value;
    }

    public static class Smr3 extends Smr {
        SmrObject3 current;
        //key，和smr1进行关联进行拼接
        HashMap<SmrObject3, SmrObject3Value> smrObject3s = new HashMap<>();

        @Override
        public void clear() {
            super.clear();
            current = null;
            smrObject3s.clear();
        }
    }

    public static class SmrObject3Value {
        int count;
        float sum;
    }

    public static class SmrObject3 {
        String id;
        String mrObjId;
        String timeStamp;
        String cgi;

        SmrObject3Value value;

        public SmrObject3(String id, String mrObjId, String timeStamp) {
            String tmpId = id;
            if (tmpId == null)
                tmpId = mrObjId;
            if (tmpId != null) {
                int pos = tmpId.indexOf(':');
                if (pos < 0)
                    cgi = tmpId;
                else
                    cgi = tmpId.substring(0, pos);
            }
            this.id = id;
            this.mrObjId = mrObjId;
            this.timeStamp = timeStamp;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((cgi == null) ? 0 : cgi.hashCode());
            result = prime * result + ((timeStamp == null) ? 0 : timeStamp.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            SmrObject3 other = (SmrObject3) obj;
            if (cgi == null) {
                if (other.cgi != null)
                    return false;
            } else if (!cgi.equals(other.cgi))
                return false;
            if (timeStamp == null) {
                if (other.timeStamp != null)
                    return false;
            } else if (!timeStamp.equals(other.timeStamp))
                return false;
            return true;
        }
    }
}

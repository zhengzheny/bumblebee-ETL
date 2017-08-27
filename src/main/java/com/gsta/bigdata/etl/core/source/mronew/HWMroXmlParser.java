package com.gsta.bigdata.etl.core.source.mronew;

import com.gsta.bigdata.etl.core.ETLData;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;

import com.gsta.bigdata.etl.core.source.mronew.MROObject.*;


/**
 * Created by tianxq on 2017/8/23.
 */
public class HWMroXmlParser extends MroXmlParser {
    public static final int FIELD_COUNT = 91;
    private static final String[] NULL_VALUES = new String[FIELD_COUNT];

    @Override
    protected void createRecords() {
        //第一段没有，不用做
        if (smr1.smrObject1s.isEmpty()) return;

        int enb_id = 0;
        String enb_this = this.enb_id;
        if (enb_this != null) {
            enb_id = toInt(this.enb_id);
        }

        final int MR_LteNcRSRP_index = getFieldIndex("MR.LteNcRSRP");
        final int MR_LteNcRSRQ_index = getFieldIndex("MR.LteNcRSRQ");
        final int MR_LteNcEarfcn_index = getFieldIndex("MR.LteNcEarfcn");
        final int MR_LteNcPci_index = getFieldIndex("MR.LteNcPci");
        final int MR_LteScRIP_index = getFieldIndex("MR.LteScRIP");
        final int MR_LteScRSRP_index = getFieldIndex("MR.LteScRSRP");
        final int MR_LteScRSRQ_index = getFieldIndex("MR.LteScRSRQ");
        final int MR_LteScTadv_index = getFieldIndex("MR.LteScTadv");
        final int MR_LteScPHR_index = getFieldIndex("MR.LteScPHR");
        final int MR_LteScPlrULQci1_index = getFieldIndex("MR.LteScPlrULQci1");
        final int MR_LteScPlrULQci2_index = getFieldIndex("MR.LteScPlrULQci2");
        final int MR_LteScPlrULQci3_index = getFieldIndex("MR.LteScPlrULQci3");
        final int MR_LteScPlrULQci4_index = getFieldIndex("MR.LteScPlrULQci4");
        final int MR_LteScPlrULQci5_index = getFieldIndex("MR.LteScPlrULQci5");
        final int MR_LteScPlrULQci6_index = getFieldIndex("MR.LteScPlrULQci6");
        final int MR_LteScPlrULQci7_index = getFieldIndex("MR.LteScPlrULQci7");
        final int MR_LteScPlrULQci8_index = getFieldIndex("MR.LteScPlrULQci8");
        final int MR_LteScPlrULQci9_index = getFieldIndex("MR.LteScPlrULQci9");
        final int MR_LteScPlrDLQci1_index = getFieldIndex("MR.LteScPlrDLQci1");
        final int MR_LteScPlrDLQci2_index = getFieldIndex("MR.LteScPlrDLQci2");
        final int MR_LteScPlrDLQci3_index = getFieldIndex("MR.LteScPlrDLQci3");
        final int MR_LteScPlrDLQci4_index = getFieldIndex("MR.LteScPlrDLQci4");
        final int MR_LteScPlrDLQci5_index = getFieldIndex("MR.LteScPlrDLQci5");
        final int MR_LteScPlrDLQci6_index = getFieldIndex("MR.LteScPlrDLQci6");
        final int MR_LteScPlrDLQci7_index = getFieldIndex("MR.LteScPlrDLQci7");
        final int MR_LteScPlrDLQci8_index = getFieldIndex("MR.LteScPlrDLQci8");
        final int MR_LteScPlrDLQci9_index = getFieldIndex("MR.LteScPlrDLQci9");
        final int MR_LteScSinrUL_index = getFieldIndex("MR.LteScSinrUL");
        final int MR_LteScEarfcn_index = getFieldIndex("MR.LteScEarfcn");
        final int MR_LteScPci_index = getFieldIndex("MR.LteScPci");
        final int MR_LteScCgi_index = getFieldIndex("MR.LteScCgi");
        final int MR_LteScAOA_index = getFieldIndex("MR.LteScAOA");
        final int MR_GsmNcellBcc_index = getFieldIndex("MR.GsmNcellBcc");
        final int MR_GsmNcellBcch_index = getFieldIndex("MR.GsmNcellBcch");
        final int MR_GsmNcellCarrierRSSI_index = getFieldIndex("MR.GsmNcellCarrierRSSI");
        final int MR_GsmNcellNcc_index = getFieldIndex("MR.GsmNcellNcc");
        final int MR_LteFddNcEarfcn_index = getFieldIndex("MR.LteFddNcEarfcn");
        final int MR_LteFddNcPci_index = getFieldIndex("MR.LteFddNcPci");
        final int MR_LteFddNcRSRP_index = getFieldIndex("MR.LteFddNcRSRP");
        final int MR_LteFddNcRSRQ_index = getFieldIndex("MR.LteFddNcRSRQ");
        final int MR_LteRSTD_index = getFieldIndex("MR.LteRSTD");
        final int MR_LteScUeRxTxTD_index = getFieldIndex("MR.LteScUeRxTxTD");
        final int MR_LteSceEuRxTxTD_index = getFieldIndex("MR.LteSceEuRxTxTD");
        final int MR_LteTEuGNSS_index = getFieldIndex("MR.LteTEuGNSS");
        final int MR_LteTUeGNSS_index = getFieldIndex("MR.LteTUeGNSS");
        final int MR_LteTddNcEarfcn_index = getFieldIndex("MR.LteTddNcEarfcn");
        final int MR_LteTddNcPci_index = getFieldIndex("MR.LteTddNcPci");
        final int MR_LteTddNcRSRP_index = getFieldIndex("MR.LteTddNcRSRP");
        final int MR_LteTddNcRSRQ_index = getFieldIndex("MR.LteTddNcRSRQ");
        final int MR_UtraCarrierRSSI_index = getFieldIndex("MR.UtraCarrierRSSI");
        final int MR_UtraCellParameterId_index = getFieldIndex("MR.UtraCellParameterId");
        final int MR_UtraCpichEcNo_index = getFieldIndex("MR.UtraCpichEcNo");
        final int MR_UtraCpichRSCP_index = getFieldIndex("MR.UtraCpichRSCP");
        final int MR_CDMANcArfcn_index = getFieldIndex("MR.CDMANcArfcn");
        final int MR_CDMANcBand_index = getFieldIndex("MR.CDMANcBand");
        final int MR_CDMANcPci_index = getFieldIndex("MR.CDMANcPci");
        final int MR_CDMAPNphase_index = getFieldIndex("MR.CDMAPNphase");
        final int MR_CDMAtype_index = getFieldIndex("MR.CDMAtype");
        final int MR_CQI_index = getFieldIndex("MR.CQI");
        final int MR_Latitude_index = getFieldIndex("MR.Latitude");
        final int MR_Longitude_index = getFieldIndex("MR.Longitude");
        final int MR_LteCDMAorHRPDNcPilotStrength_index = getFieldIndex("MR.LteCDMAorHRPDNcPilotStrength");
        final int MR_LteScBSR_index = getFieldIndex("MR.LteScBSR");
        final int MR_LteScPDSCHPRBNum_index = getFieldIndex("MR.LteScPDSCHPRBNum");
        final int MR_LteScPUSCHPRBNum_index = getFieldIndex("MR.LteScPUSCHPRBNum");
        final int MR_LteScRI_index = getFieldIndex("MR.LteScRI");

        for (SmrObject1 smrObject1 : smr1.smrObject1s) {
            ArrayList<String[]> values = smrObject1.values;
            if (values.isEmpty()) continue;

            String[] first = values.get(0);
            int size = values.size();
            if (size > 1) {
                // 降序排列
                Collections.sort(values, new Comparator<String[]>() {
                    @Override
                    public int compare(String[] o1, String[] o2) {
                        int LteNcRSRP1 = toInt(o1[MR_LteNcRSRP_index]);
                        int LteNcRSRP2 = toInt(o2[MR_LteNcRSRP_index]);
                        return Integer.signum(LteNcRSRP2 - LteNcRSRP1);
                    }
                });
            }
            String MR_LteNcRSRP1 = values.get(0)[MR_LteNcRSRP_index];
            String MR_LteNcRSRP2 = size > 1 ? values.get(1)[MR_LteNcRSRP_index] : null;
            String MR_LteNcRSRP3 = size > 2 ? values.get(2)[MR_LteNcRSRP_index] : null;
            String MR_LteNcRSRP4 = size > 3 ? values.get(3)[MR_LteNcRSRP_index] : null;
            String MR_LteNcRSRP5 = size > 4 ? values.get(4)[MR_LteNcRSRP_index] : null;
            String MR_LteNcRSRP6 = size > 5 ? values.get(5)[MR_LteNcRSRP_index] : null;
            String MR_LteNcRSRP7 = size > 6 ? values.get(6)[MR_LteNcRSRP_index] : null;
            String MR_LteNcRSRP8 = size > 7 ? values.get(7)[MR_LteNcRSRP_index] : null;
            String MR_LteNcRSRP9 = size > 8 ? values.get(8)[MR_LteNcRSRP_index] : null;
            String MR_LteNcRSRP10 = size > 9 ? values.get(9)[MR_LteNcRSRP_index] : null;
            String MR_LteNcRSRP11 = size > 10 ? values.get(10)[MR_LteNcRSRP_index] : null;
            String MR_LteNcRSRP12 = size > 11 ? values.get(11)[MR_LteNcRSRP_index] : null;
            String MR_LteNcRSRP13 = size > 12 ? values.get(12)[MR_LteNcRSRP_index] : null;
            String MR_LteNcRSRP14 = size > 13 ? values.get(13)[MR_LteNcRSRP_index] : null;
            String MR_LteNcRSRP15 = size > 14 ? values.get(14)[MR_LteNcRSRP_index] : null;

            String MR_LteNcRSRQ1 = values.get(0)[MR_LteNcRSRQ_index];
            String MR_LteNcRSRQ2 = size > 1 ? values.get(1)[MR_LteNcRSRQ_index] : null;
            String MR_LteNcRSRQ3 = size > 2 ? values.get(2)[MR_LteNcRSRQ_index] : null;
            String MR_LteNcRSRQ4 = size > 3 ? values.get(3)[MR_LteNcRSRQ_index] : null;
            String MR_LteNcRSRQ5 = size > 4 ? values.get(4)[MR_LteNcRSRQ_index] : null;
            String MR_LteNcRSRQ6 = size > 5 ? values.get(5)[MR_LteNcRSRQ_index] : null;
            String MR_LteNcRSRQ7 = size > 6 ? values.get(6)[MR_LteNcRSRQ_index] : null;
            String MR_LteNcRSRQ8 = size > 7 ? values.get(7)[MR_LteNcRSRQ_index] : null;
            String MR_LteNcRSRQ9 = size > 8 ? values.get(8)[MR_LteNcRSRQ_index] : null;
            String MR_LteNcRSRQ10 = size > 9 ? values.get(9)[MR_LteNcRSRQ_index] : null;
            String MR_LteNcRSRQ11 = size > 10 ? values.get(10)[MR_LteNcRSRQ_index] : null;
            String MR_LteNcRSRQ12 = size > 11 ? values.get(11)[MR_LteNcRSRQ_index] : null;
            String MR_LteNcRSRQ13 = size > 12 ? values.get(12)[MR_LteNcRSRQ_index] : null;
            String MR_LteNcRSRQ14 = size > 13 ? values.get(13)[MR_LteNcRSRQ_index] : null;
            String MR_LteNcRSRQ15 = size > 14 ? values.get(14)[MR_LteNcRSRQ_index] : null;

            String MR_LteNcEarfcn1 = values.get(0)[MR_LteNcEarfcn_index];
            String MR_LteNcEarfcn2 = size > 1 ? values.get(1)[MR_LteNcEarfcn_index] : null;
            String MR_LteNcEarfcn3 = size > 2 ? values.get(2)[MR_LteNcEarfcn_index] : null;
            String MR_LteNcEarfcn4 = size > 3 ? values.get(3)[MR_LteNcEarfcn_index] : null;
            String MR_LteNcEarfcn5 = size > 4 ? values.get(4)[MR_LteNcEarfcn_index] : null;
            String MR_LteNcEarfcn6 = size > 5 ? values.get(5)[MR_LteNcEarfcn_index] : null;
            String MR_LteNcEarfcn7 = size > 6 ? values.get(6)[MR_LteNcEarfcn_index] : null;
            String MR_LteNcEarfcn8 = size > 7 ? values.get(7)[MR_LteNcEarfcn_index] : null;
            String MR_LteNcEarfcn9 = size > 8 ? values.get(8)[MR_LteNcEarfcn_index] : null;
            String MR_LteNcEarfcn10 = size > 9 ? values.get(9)[MR_LteNcEarfcn_index] : null;
            String MR_LteNcEarfcn11 = size > 10 ? values.get(10)[MR_LteNcEarfcn_index] : null;
            String MR_LteNcEarfcn12 = size > 11 ? values.get(11)[MR_LteNcEarfcn_index] : null;
            String MR_LteNcEarfcn13 = size > 12 ? values.get(12)[MR_LteNcEarfcn_index] : null;
            String MR_LteNcEarfcn14 = size > 13 ? values.get(13)[MR_LteNcEarfcn_index] : null;
            String MR_LteNcEarfcn15 = size > 14 ? values.get(14)[MR_LteNcEarfcn_index] : null;

            String MR_LteNcPci1 = values.get(0)[MR_LteNcPci_index];
            String MR_LteNcPci2 = size > 1 ? values.get(1)[MR_LteNcPci_index] : null;
            String MR_LteNcPci3 = size > 2 ? values.get(2)[MR_LteNcPci_index] : null;
            String MR_LteNcPci4 = size > 3 ? values.get(3)[MR_LteNcPci_index] : null;
            String MR_LteNcPci5 = size > 4 ? values.get(4)[MR_LteNcPci_index] : null;
            String MR_LteNcPci6 = size > 5 ? values.get(5)[MR_LteNcPci_index] : null;
            String MR_LteNcPci7 = size > 6 ? values.get(6)[MR_LteNcPci_index] : null;
            String MR_LteNcPci8 = size > 7 ? values.get(7)[MR_LteNcPci_index] : null;
            String MR_LteNcPci9 = size > 8 ? values.get(8)[MR_LteNcPci_index] : null;
            String MR_LteNcPci10 = size > 9 ? values.get(9)[MR_LteNcPci_index] : null;
            String MR_LteNcPci11 = size > 10 ? values.get(10)[MR_LteNcPci_index] : null;
            String MR_LteNcPci12 = size > 11 ? values.get(11)[MR_LteNcPci_index] : null;
            String MR_LteNcPci13 = size > 12 ? values.get(12)[MR_LteNcPci_index] : null;
            String MR_LteNcPci14 = size > 13 ? values.get(13)[MR_LteNcPci_index] : null;
            String MR_LteNcPci15 = size > 14 ? values.get(14)[MR_LteNcPci_index] : null;

            String[] v1 = first;
            String[] v2;
            String MR_LteScRIP = null;
            if (!smr2.smrObject2s.isEmpty()) {
                String[] smrObject2Value = smr2.smrObject2s.get(smrObject1);
                if (smrObject2Value == null) {
                    smrObject2Value = NULL_VALUES;
                }
                SmrObject3Value smrObject3Value = smr3.smrObject3s
                        .get(new SmrObject3(smrObject1.id, smrObject1.mrObjId, smrObject1.timeStamp));
                if (smrObject3Value != null)
                    MR_LteScRIP = Float.toString(smrObject3Value.sum / smrObject3Value.count);
                v2 = smrObject2Value;
            } else {
                v2 = first;
                MR_LteScRIP = first[MR_LteScRIP_index];
            }

            ETLData data = new ETLData();

            data.addData(STARTTIME, startTime);
            data.addData("endTime", endTime);
            data.addData("TimeStamp", df_a.format(new Date(toTime(smrObject1.timeStamp))));
            data.addData(ENODEID, String.valueOf(enb_id));

            int cell_id = -1; // cell_id有可能为0的
            String id = smrObject1.id;
            if (id != null) {
                cell_id = toInt(id);
            }
            data.addData("CELLID", String.valueOf(cell_id));

            data.addData("MmeGroupId", smrObject1.mmeGroupId);
            data.addData("MmeUeS1apId", smrObject1.mmeUeS1apId);
            data.addData("MmeCode", smrObject1.mmeCode);

            data.addData("MR_LteScRSRP", v(v1, MR_LteScRSRP_index));
            data.addData("MR_LteNcRSRP1", MR_LteNcRSRP1);
            data.addData("MR_LteNcRSRP2", MR_LteNcRSRP2);
            data.addData("MR_LteNcRSRP3", MR_LteNcRSRP3);
            data.addData("MR_LteNcRSRP4", MR_LteNcRSRP4);
            data.addData("MR_LteNcRSRP5", MR_LteNcRSRP5);
            data.addData("MR_LteScRSRQ", v(v1, MR_LteScRSRQ_index));
            data.addData("MR_LteNcRSRQ1", MR_LteNcRSRQ1);
            data.addData("MR_LteNcRSRQ2", MR_LteNcRSRQ2);
            data.addData("MR_LteNcRSRQ3", MR_LteNcRSRQ3);
            data.addData("MR_LteNcRSRQ4", MR_LteNcRSRQ4);
            data.addData("MR_LteNcRSRQ5", MR_LteNcRSRQ5);
            data.addData("MR_LteScTadv", v(v1, MR_LteScTadv_index));
            data.addData("MR_LteScPHR", v(v1, MR_LteScPHR_index));
            data.addData("MR_LteScRIP", MR_LteScRIP);
            data.addData("MR_LteScPlrULQci1", v(v2, MR_LteScPlrULQci1_index));
            data.addData("MR_LteScPlrULQci2", v(v2, MR_LteScPlrULQci2_index));
            data.addData("MR_LteScPlrULQci3", v(v2, MR_LteScPlrULQci3_index));
            data.addData("MR_LteScPlrULQci4", v(v2, MR_LteScPlrULQci4_index));
            data.addData("MR_LteScPlrULQci5", v(v2, MR_LteScPlrULQci5_index));
            data.addData("MR_LteScPlrULQci6", v(v2, MR_LteScPlrULQci6_index));
            data.addData("MR_LteScPlrULQci7", v(v2, MR_LteScPlrULQci7_index));
            data.addData("MR_LteScPlrULQci8", v(v2, MR_LteScPlrULQci8_index));
            data.addData("MR_LteScPlrULQci9", v(v2, MR_LteScPlrULQci9_index));
            data.addData("MR_LteScPlrDLQci1", v(v2, MR_LteScPlrDLQci1_index));
            data.addData("MR_LteScPlrDLQci2", v(v2, MR_LteScPlrDLQci2_index));
            data.addData("MR_LteScPlrDLQci3", v(v2, MR_LteScPlrDLQci3_index));
            data.addData("MR_LteScPlrDLQci4", v(v2, MR_LteScPlrDLQci4_index));
            data.addData("MR_LteScPlrDLQci5", v(v2, MR_LteScPlrDLQci5_index));
            data.addData("MR_LteScPlrDLQci6", v(v2, MR_LteScPlrDLQci6_index));
            data.addData("MR_LteScPlrDLQci7", v(v2, MR_LteScPlrDLQci7_index));
            data.addData("MR_LteScPlrDLQci8", v(v2, MR_LteScPlrDLQci8_index));
            data.addData("MR_LteScPlrDLQci9", v(v2, MR_LteScPlrDLQci9_index));
            data.addData("MR_LteScSinrUL", v(v1, MR_LteScSinrUL_index));
            data.addData("MR_LteScEarfcn", v(v1, MR_LteScEarfcn_index));
            data.addData("MR_LteScPci", v(v1, MR_LteScPci_index));
            data.addData("MR_LteScCgi", v(v1, MR_LteScCgi_index));
            data.addData("MR_LteNcEarfcn1", MR_LteNcEarfcn1);
            data.addData("MR_LteNcEarfcn2", MR_LteNcEarfcn2);
            data.addData("MR_LteNcEarfcn3", MR_LteNcEarfcn3);
            data.addData("MR_LteNcEarfcn4", MR_LteNcEarfcn4);
            data.addData("MR_LteNcEarfcn5", MR_LteNcEarfcn5);
            data.addData("MR_LteNcPci1", MR_LteNcPci1);
            data.addData("MR_LteNcPci2", MR_LteNcPci2);
            data.addData("MR_LteNcPci3", MR_LteNcPci3);
            data.addData("MR_LteNcPci4", MR_LteNcPci4);
            data.addData("MR_LteNcPci5", MR_LteNcPci5);
            data.addData("MR_LteScAOA", v(v1, MR_LteScAOA_index));
            data.addData("MR_GsmNcellBcc", v(first, MR_GsmNcellBcc_index));
            data.addData("MR_GsmNcellBcch", v(first, MR_GsmNcellBcch_index));
            data.addData("MR_GsmNcellCarrierRSSI", v(first, MR_GsmNcellCarrierRSSI_index));
            data.addData("MR_GsmNcellNcc", v(first, MR_GsmNcellNcc_index));
            data.addData("MR_LteFddNcEarfcn", v(first, MR_LteFddNcEarfcn_index));
            data.addData("MR_LteFddNcPci", v(first, MR_LteFddNcPci_index));
            data.addData("MR_LteFddNcRSRP", v(first, MR_LteFddNcRSRP_index));
            data.addData("MR_LteFddNcRSRQ", v(first, MR_LteFddNcRSRQ_index));
            data.addData("MR_LteRSTD", v(first, MR_LteRSTD_index));
            data.addData("MR_LteScUeRxTxTD", v(first, MR_LteScUeRxTxTD_index));
            data.addData("MR_LteSceEuRxTxTD", v(first, MR_LteSceEuRxTxTD_index));
            data.addData("MR_LteTEuGNSS", v(first, MR_LteTEuGNSS_index));
            data.addData("MR_LteTUeGNSS", v(first, MR_LteTUeGNSS_index));
            data.addData("MR_LteTddNcEarfcn", v(first, MR_LteTddNcEarfcn_index));
            data.addData("MR_LteTddNcPci", v(first, MR_LteTddNcPci_index));
            data.addData("MR_LteTddNcRSRP", v(first, MR_LteTddNcRSRP_index));
            data.addData("MR_LteTddNcRSRQ", v(first, MR_LteTddNcRSRQ_index));
            data.addData("MR_UtraCarrierRSSI", v(first, MR_UtraCarrierRSSI_index));
            data.addData("MR_UtraCellParameterId", v(first, MR_UtraCellParameterId_index));
            data.addData("MR_UtraCpichEcNo", v(first, MR_UtraCpichEcNo_index));
            data.addData("MR_UtraCpichRSCP", v(first, MR_UtraCpichRSCP_index));
            data.addData("MR_CDMANcArfcn", v(first, MR_CDMANcArfcn_index));
            data.addData("MR_CDMANcBand", v(first, MR_CDMANcBand_index));
            data.addData("MR_CDMANcPci", v(first, MR_CDMANcPci_index));
            data.addData("MR_CDMAPNphase", v(first, MR_CDMAPNphase_index));
            data.addData("MR_CDMAtype", v(first, MR_CDMAtype_index));
            data.addData("MR_CQI", v(first, MR_CQI_index));
            data.addData("MR_Latitude", v(first, MR_Latitude_index));
            data.addData("MR_Longitude", v(first, MR_Longitude_index));
            data.addData("MR_LteCDMAorHRPDNcPilotStrength", v(first, MR_LteCDMAorHRPDNcPilotStrength_index));
            data.addData("MR_LteScBSR", v(first, MR_LteScBSR_index));
            data.addData("MR_LteScPDSCHPRBNum", v(first, MR_LteScPDSCHPRBNum_index));
            data.addData("MR_LteScPUSCHPRBNum", v(first, MR_LteScPUSCHPRBNum_index));
            data.addData("MR_LteScRI", v(first, MR_LteScRI_index));

            int flag = (isInvalid(smrObject1.timeStamp) ? 1 : 0) //
                    + (enb_id <= 0 ? 2 : 0) //
                    + (cell_id < 0 ? 4 : 0) // cell_id有可能为0的
                    + (isInvalid(smrObject1.mmeUeS1apId) ? 8 : 0) //
                    + (isInvalid(smrObject1.mmeGroupId) ? 16 : 0) //
                    + (isInvalid(smrObject1.mmeCode) ? 32 : 0) //
                    + (isNullOrEmpty(v(v1, MR_LteScRSRP_index)) ? 64 : 0) //
                    + (isNullOrEmpty(v(v1, MR_LteScRSRQ_index)) ? 128 : 0); //

            data.addData("FLAG", String.valueOf(flag));
            data.addData("MR_LteNcRSRP6", MR_LteNcRSRP6);
            data.addData("MR_LteNcRSRP7", MR_LteNcRSRP7);
            data.addData("MR_LteNcRSRP8", MR_LteNcRSRP8);
            data.addData("MR_LteNcRSRP9", MR_LteNcRSRP9);
            data.addData("MR_LteNcRSRP10", MR_LteNcRSRP10);
            data.addData("MR_LteNcRSRP11", MR_LteNcRSRP11);
            data.addData("MR_LteNcRSRP12", MR_LteNcRSRP12);
            data.addData("MR_LteNcRSRP13", MR_LteNcRSRP13);
            data.addData("MR_LteNcRSRP14", MR_LteNcRSRP14);
            data.addData("MR_LteNcRSRP15", MR_LteNcRSRP15);
            data.addData("MR_LteNcEarfcn6", MR_LteNcEarfcn6);
            data.addData("MR_LteNcEarfcn7", MR_LteNcEarfcn7);
            data.addData("MR_LteNcEarfcn8", MR_LteNcEarfcn8);
            data.addData("MR_LteNcEarfcn9", MR_LteNcEarfcn9);
            data.addData("MR_LteNcEarfcn10", MR_LteNcEarfcn10);
            data.addData("MR_LteNcEarfcn11", MR_LteNcEarfcn11);
            data.addData("MR_LteNcEarfcn12", MR_LteNcEarfcn12);
            data.addData("MR_LteNcEarfcn13", MR_LteNcEarfcn13);
            data.addData("MR_LteNcEarfcn14", MR_LteNcEarfcn14);
            data.addData("MR_LteNcEarfcn15", MR_LteNcEarfcn15);
            data.addData("MR_LteNcPci6", MR_LteNcPci6);
            data.addData("MR_LteNcPci7", MR_LteNcPci7);
            data.addData("MR_LteNcPci8", MR_LteNcPci8);
            data.addData("MR_LteNcPci9", MR_LteNcPci9);
            data.addData("MR_LteNcPci10", MR_LteNcPci10);
            data.addData("MR_LteNcPci11", MR_LteNcPci11);
            data.addData("MR_LteNcPci12", MR_LteNcPci12);
            data.addData("MR_LteNcPci13", MR_LteNcPci13);
            data.addData("MR_LteNcPci14", MR_LteNcPci14);
            data.addData("MR_LteNcPci15", MR_LteNcPci15);
            data.addData("MR_LteNcRSRQ6", MR_LteNcRSRQ6);
            data.addData("MR_LteNcRSRQ7", MR_LteNcRSRQ7);
            data.addData("MR_LteNcRSRQ8", MR_LteNcRSRQ8);
            data.addData("MR_LteNcRSRQ9", MR_LteNcRSRQ9);
            data.addData("MR_LteNcRSRQ10", MR_LteNcRSRQ10);
            data.addData("MR_LteNcRSRQ11", MR_LteNcRSRQ11);
            data.addData("MR_LteNcRSRQ12", MR_LteNcRSRQ12);
            data.addData("MR_LteNcRSRQ13", MR_LteNcRSRQ13);
            data.addData("MR_LteNcRSRQ14", MR_LteNcRSRQ14);
            data.addData("MR_LteNcRSRQ15", MR_LteNcRSRQ15);

            lstEtlData.add(data);
        }
    }
}

package com.gsta.bigdata.utils;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
/**
 * 
 * @author tianxq
 *
 */
public class IDCardUtil {
	final static Map<Integer, String> zoneNum = new HashMap<Integer, String>();
	static {
		zoneNum.put(11, "beijing");
		zoneNum.put(12, "tianjing");
		zoneNum.put(13, "hebei");
		zoneNum.put(14, "shanxi");
		zoneNum.put(15, "neimenggu");
		zoneNum.put(21, "liaoning");
		zoneNum.put(22, "jiling");
		zoneNum.put(23, "heilongjiang");
		zoneNum.put(31, "shanghai");
		zoneNum.put(32, "jiangsu");
		zoneNum.put(33, "zhejiang");
		zoneNum.put(34, "anhui");
		zoneNum.put(35, "fujian");
		zoneNum.put(36, "jiangxi");
		zoneNum.put(37, "shandong");
		zoneNum.put(41, "henan");
		zoneNum.put(42, "hubei");
		zoneNum.put(43, "hunan");
		zoneNum.put(44, "guangdong");
		zoneNum.put(45, "guangxi");
		zoneNum.put(46, "hainan");
		zoneNum.put(50, "chongqing");
		zoneNum.put(51, "sichuan");
		zoneNum.put(52, "guizhou");
		zoneNum.put(53, "yunnan");
		zoneNum.put(54, "xizang");
		zoneNum.put(61, "shanxi");
		zoneNum.put(62, "ganshu");
		zoneNum.put(63, "qinghai");
		zoneNum.put(64, "xinjiang");
		zoneNum.put(71, "taiwan");
		zoneNum.put(81, "hongkong");
		zoneNum.put(82, "macao");
		zoneNum.put(91, "foreigner");
	}

	final static int[] PARITYBIT = { '1', '0', 'X', '9', '8', '7', '6', '5',
			'4', '3', '2' };
	final static int[] POWER_LIST = { 7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10,
			5, 8, 4, 2 };

	
	public static boolean isIDCard(String certNo) {
		if (certNo == null || (certNo.length() != 15 && certNo.length() != 18))
			return false;
		final char[] cs = certNo.toUpperCase().toCharArray();
		// verify bits
		int power = 0;
		for (int i = 0; i < cs.length; i++) {
			if (i == cs.length - 1 && cs[i] == 'X')
				break;// last bit is X or x
			if (cs[i] < '0' || cs[i] > '9')
				return false;
			if (i < cs.length - 1) {
				power += (cs[i] - '0') * POWER_LIST[i];
			}
		}

		//verify zone num
		if (!zoneNum.containsKey(Integer.valueOf(certNo.substring(0, 2)))) {
			return false;
		}

		//verify year
		String year = certNo.length() == 15 ? getIdcardCalendar()
				+ certNo.substring(6, 8) : certNo.substring(6, 10);

		final int iyear = Integer.parseInt(year);
		if (iyear < 1900 || iyear > Calendar.getInstance().get(Calendar.YEAR))
			return false;

		//verify month
		String month = certNo.length() == 15 ? certNo.substring(8, 10) : certNo
				.substring(10, 12);
		final int imonth = Integer.parseInt(month);
		if (imonth < 1 || imonth > 12) {
			return false;
		}

		//verify day
		String day = certNo.length() == 15 ? certNo.substring(10, 12) : certNo
				.substring(12, 14);
		final int iday = Integer.parseInt(day);
		if (iday < 1 || iday > 31)
			return false;

		//very check code
		if (certNo.length() == 15)
			return true;
		return cs[cs.length - 1] == PARITYBIT[power % 11];
	}

	private static int getIdcardCalendar() {
		GregorianCalendar curDay = new GregorianCalendar();
		int curYear = curDay.get(Calendar.YEAR);
		int year2bit = Integer.parseInt(String.valueOf(curYear).substring(2));
		return year2bit;
	}

	public static void main(String[] args) {
		String idCard = "450981198802261753";
		boolean mark = IDCardUtil.isIDCard(idCard);
		System.out.println(mark);
	}
}

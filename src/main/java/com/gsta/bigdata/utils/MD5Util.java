package com.gsta.bigdata.utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5Util {
	/**
	 * 对字符串右边进行脱敏处理
	 * @param srcTxt：原始字符串
	 * @param rightLen：字符串右边指定长度
	 * @param replaceStr： 用来替换的占位字符
	 * @param reverseFlag： 是否反转处理。1-对字符串除右边指定长度的内容之外作替换处理；0-对字符串右边指定长度的内容作替换处理
	 * @return
	 */
	public static String rmask(String srcTxt, long rightLen, String replaceStr, int reverseFlag) {
		if (srcTxt == null || srcTxt.length() == 0 || rightLen <=0)
		{
			return srcTxt;
		}
		int srcLen = srcTxt.length();
		int replaceLen = 0;
		StringBuffer buf = new StringBuffer(100);
		if (reverseFlag > 0) //反转脱敏
		{
			replaceLen = (int)(srcLen - rightLen);
			if (replaceLen < 1)
				return srcTxt;
			else
			{
		        for (int i = 0; i < replaceLen; i++)
		        {
		        	buf.append(replaceStr);
		        }
		        buf.append(srcTxt.substring(replaceLen));
		        return buf.toString();
			}
		}
		else { //正序脱敏
			replaceLen = (int)rightLen;
			if (rightLen > srcLen)
			{
				replaceLen = srcLen;
			}
			if (replaceLen < 1)
				return srcTxt;
			else
			{
		        buf.append(srcTxt.substring(0, srcLen-replaceLen));
		        for (int i = 0; i < replaceLen; i++)
		        {
		        	buf.append(replaceStr);
		        }
		        return buf.toString();
			}
		}
	}
	
	/**
	 * 对字符串左边进行脱敏处理
	 * @param srcTxt：原始字符串
	 * @param leftLen：字符串左边指定长度
	 * @param replaceStr： 用来替换的占位字符
	 * @param reverseFlag： 是否反转处理。1-对字符串除左边指定长度的内容之外作替换处理；0-对字符串左边指定长度的内容作替换处理
	 * @return
	 */
	public static String lmask(String srcTxt, long leftLen, String replaceStr, int reverseFlag) {
		if (srcTxt == null || srcTxt.length() == 0 || leftLen <=0)
		{
			return srcTxt;
		}
		int srcLen = srcTxt.length();
		int replaceLen = 0;
		StringBuffer buf = new StringBuffer(100);
		if (reverseFlag > 0) //反转脱敏
		{
			replaceLen = (int)(srcLen - leftLen);
			if (replaceLen < 1)
				return srcTxt;
			else
			{
		        buf.append(srcTxt.substring(0, srcLen-replaceLen));
		        for (int i = 0; i < replaceLen; i++)
		        {
		        	buf.append(replaceStr);
		        }
		        return buf.toString();
			}
		}
		else { //正序脱敏
			replaceLen = (int)leftLen;
			if (leftLen > srcLen)
			{
				replaceLen = srcLen;
			}
			if (replaceLen < 1)
				return srcTxt;
			else
			{
		        for (int i = 0; i < replaceLen; i++)
		        {
		        	buf.append(replaceStr);
		        }
		        buf.append(srcTxt.substring(replaceLen));
		        return buf.toString();
			}
		}
	}

	/**
	 * 对字符串首尾进行脱敏处理
	 * @param srcTxt：原始字符串
	 * @param leftLen：字符串左边指定长度
	 * @param rightLen：字符串右边指定长度
	 * @param replaceStr： 用来替换的占位字符
	 * @param reverseFlag： 是否反转处理。1-对字符串除首尾指定长度的内容之外作替换处理；0-对字符串首尾指定长度的内容作替换处理
	 * @return
	 */
	public static String mask(String srcTxt, long leftLen, long rightLen, String replaceStr, int reverseFlag) {
		if (srcTxt == null || srcTxt.length() == 0 || leftLen + rightLen > srcTxt.length())
		{
			return srcTxt;
		}

		int srcLen = srcTxt.length();
		int replaceLen = 0;
		StringBuffer buf = new StringBuffer(100);
		if (reverseFlag > 0) //反转脱敏
		{
			replaceLen = srcLen;
			if (leftLen > 0)
			{
				replaceLen = replaceLen - (int)leftLen;
			}
			if (rightLen > 0)
			{
				replaceLen = replaceLen - (int)rightLen;
			}
			
			if (replaceLen < 1)
			{
				return srcTxt;
			}
			else
			{
				if (leftLen > 0)
				{
			        buf.append(srcTxt.substring(0, (int)leftLen));
				}
		        for (int i = 0; i < replaceLen; i++)
		        {
		        	buf.append(replaceStr);
		        }
				if (rightLen > 0)
				{
			        buf.append(srcTxt.substring(srcLen-(int)rightLen));
				}
		        return buf.toString();
			}
		}
		else { //正序脱敏
			replaceLen = (int)(srcLen - leftLen - rightLen);
			if (replaceLen < 1)
			{
				return srcTxt;
			}
			else
			{
		        for (int i = 0; i < leftLen; i++)
		        {
		        	buf.append(replaceStr);
		        }
		        
		        // 中间部分脱敏
		        if (leftLen > 0 && rightLen > 0)
		        {
			        buf.append(srcTxt.substring((int)leftLen, srcLen-(int)rightLen));
		        }
		        else if (leftLen < 1 && rightLen > 0)
		        {
			        buf.append(srcTxt.substring(0, srcLen-(int)rightLen));
		        }
		        else if (leftLen > 0 && rightLen < 1)
		        {
			        buf.append(srcTxt.substring((int)leftLen, srcLen));
		        }

		        for (int i = 0; i < rightLen; i++)
		        {
		        	buf.append(replaceStr);
		        }
		        return buf.toString();
			}
		}
	}
	
	/**
	 * 对字符串MD5加密
	 * @param srcStr
	 * @return
	 */
	public static String md5(String srcStr) {
		if (srcStr == null || srcStr.length() == 0)
		{
			return srcStr;
		}
		MessageDigest md5 = null;
		StringBuffer hexValue = new StringBuffer(35);
		StringBuffer tmpSrc = new StringBuffer(srcStr.length());
		int reverseLen = (int)Math.ceil(srcStr.length() / 3);
		tmpSrc.append(srcStr.substring(reverseLen));
		tmpSrc.append(srcStr.substring(0, reverseLen));
		try {
			md5 = MessageDigest.getInstance("MD5");
		} catch (Exception e) {
			System.out.println(e.toString());
			return "";
		}
		char[] charArray = tmpSrc.toString().toCharArray();
		byte[] byteArray = new byte[charArray.length];

		for (int i = 0; i < charArray.length; i++)
			byteArray[i] = (byte) charArray[i];

		byte[] md5Bytes = md5.digest(byteArray);
		for (int i = 0; i < md5Bytes.length; i++) {
			int val = ((int) md5Bytes[i]) & 0xff;
			if (val < 16) hexValue.append("0");
			hexValue.append(Integer.toHexString(val));
		}
		return hexValue.toString().toUpperCase();
	}
	
	
	private static final char[] HEXS = {'0', '1', '2', '3', '4', '5', '6', '7' ,'8','9','A','B','C','D','E','F'};
	private static final ThreadLocal<MessageDigest> MD5 = new ThreadLocal<MessageDigest>() {
		public MessageDigest initialValue() {
			try {
				return MessageDigest.getInstance("MD5");
			} catch (NoSuchAlgorithmException e) {
				return null;
			}
		}
	};

	
	public static String md52(String srcStr)
	{
		if (srcStr == null || srcStr.length() == 0)
		{
			return srcStr;
		}
		StringBuilder tmpSrc = new StringBuilder(srcStr.length());
		int reverseLen = (int)Math.ceil(srcStr.length() / 3);
		tmpSrc.append(srcStr.substring(reverseLen));
		tmpSrc.append(srcStr.substring(0, reverseLen));
		byte[] d = tmpSrc.toString().getBytes();
		MessageDigest md = MD5.get();
		try{
			for(int i = 32,j = 0; j < 41; i++,j++) {
				if(d[i] == '\0') break;
				md.update(d[i]);
			}
		}catch (Exception e)
		{
		}
		byte[] md5 = md.digest();
		StringBuilder sb = new StringBuilder();
		for (byte b : md5) sb.append(HEXS[b >> 4 & 0xf]).append(HEXS[b & 0xf]);
		String md5name = sb.toString();
		return md5name;
	}
}

package com.gsta.bigdata.utils;

import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
/**
 * 
 * @author tianxq
 *
 */
public class StringUtils {
	 private static final Pattern charCheckPatn = Pattern.compile("\\s*|\t*|\r*|\n*", Pattern.CASE_INSENSITIVE);
	 
	public static String getPackageName(Class<?> clazz) {
		String className = clazz.getName();
		String simpleName = clazz.getSimpleName();
		return className.substring(0, className.length() - simpleName.length());
	}

	public static String upperCaseFirstChar(String str) {
		String ret = str;
		if (ret != null) {
			ret = ret.replaceFirst(ret.substring(0, 1), ret.substring(0, 1)
					.toUpperCase());
		}

		return ret;
	}

	public static boolean isChinese(String strOne) {
		if (null != strOne || !"".equals(strOne)) {
			if (strOne.matches("[\\u4e00-\\u9fbb]+")) {
				return true;
			}
		}
		return false;
	}

	public static String rightTrim(String str) {
		String ret = null;
		if (null != str) {
			ret = str.substring(0, str.lastIndexOf(str.trim())
					+ str.trim().length());
		}
		return ret;
	}

	public static boolean isDigit(String strNum) {
		Pattern pattern = Pattern.compile("[0-9]{1,}");
		Matcher matcher = pattern.matcher((CharSequence) strNum);
		return matcher.matches();
	}

	public static String getNumbers(String content) {
		Pattern pattern = Pattern.compile("\\d+");
		Matcher matcher = pattern.matcher(content);
		while (matcher.find()) {
			return matcher.group(0);
		}
		return "";
	}

	public static String getNotNumbers(String content) {
		Pattern pattern = Pattern.compile("\\D+");
		Matcher matcher = pattern.matcher(content);
		while (matcher.find()) {
			return matcher.group(0);
		}
		return "";
	}

	/**
	 * @author tianxq split string to string array according delimiter and
	 *         wrapper.
	 * 
	 *         for example: 0.if has no wrapper,split by delimiter
	 * 
	 *         1.if field has full wrapper which means has couple wrapper, will
	 *         ignore delimiter between wrapper String str =
	 *         "a,'b','c,d',e,,'f'"; split result:String[]
	 *         ={"a","'b'","'c,d'","e","","'f'"}
	 * 
	 *         2.if field hasn't full wrapper which means has only wrapper, will
	 *         ignore delimiter and wrapper until the end of string String str =
	 *         "a,'b','c,d,'e'"; split result:String[] = {"a","'b'","'c,d,'e'"}
	 * 
	 * @param str
	 * @param delimiter
	 * @param wrapper
	 * @return
	 */
	public static String[] splitByWrapper(String str, String delimiter,
			String wrapper) {
		if (str == null || delimiter == null) {
			return null;
		}

		if (wrapper == null) {
			return str.split(delimiter, -1);
		}

		String field = "";
		boolean wrapperflag = false;
		ArrayList<String> arrFields = new ArrayList<String>();
		for (int i = 0; i < str.length(); i++) {
			char[] arrChar = { str.charAt(i) };
			String c = new String(arrChar);
			if (wrapperflag) {
				if (wrapper.equals(c)) {
					wrapperflag = false;
				}
				field = field + c;
			} else {
				if (delimiter.equals(c)) {
					arrFields.add(field);
					field = "";
				} else if (wrapper.equals(c)) {
					field = field + c;
					wrapperflag = true;
				} else {
					field = field + c;
				}
			}
		}

		// the last field
		arrFields.add(field);

		String[] ret = new String[arrFields.size()];
		arrFields.toArray(ret);
		return ret;
	}

	public static String getHost(String url) {
		if (url == null || url.trim().equals("")) {
			return "";
		}
		String host = "";
		Pattern p = Pattern.compile("(?<=//|)((\\w)+\\.)+\\w+");
		Matcher matcher = p.matcher(url);
		if (matcher.find()) {
			host = matcher.group();
		}
		return host;
	}

	public static String byte2Str(byte[] bytes, String charset)
			throws UnsupportedEncodingException {
		String str = null;

		if (charset == null) {
			str = new String(bytes);
		} else {
			str = new String(bytes, charset);
		}

		return str;
	}

	public static String dateFormat(String dateStr, String oldPattern,
			String newPattern) throws ParseException {
		if (null == dateStr || null == oldPattern || null == newPattern) {
			return null;
		}
		DateFormat oldFormat = new SimpleDateFormat(oldPattern);
		DateFormat newFormat = new SimpleDateFormat(newPattern);
		Date oldDate = oldFormat.parse(dateStr);
		return newFormat.format(oldDate);
	}
	
	public static boolean isMessyCode(String strName) {    
		if (strName == null)
		{
			return false;
		}
		Matcher m = charCheckPatn.matcher(strName);    
		String after = m.replaceAll("");    
		String temp = after.replaceAll("\\p{P}", "");    
		char[] ch = temp.trim().toCharArray();    
		float chLength = ch.length;    
		float count = 0;    
		for (int i = 0; i < ch.length; i++) {    
			char c = ch[i];    
			if (!Character.isLetterOrDigit(c)) {    
				if (!isChinese(c)) {    
					count = count + 1;    
				}    
			} 
		}    
		float result = count / chLength;    
		if (result > 0.4) {    
			return true;    
		} else {    
			return false;    
		}   	
	}
	
    public static boolean isChinese(char c) {   
        Character.UnicodeScript sc = Character.UnicodeScript.of(c);
        if (sc == Character.UnicodeScript.HAN) {
            return true;
        }
        return false;    	
	}    

    public static boolean isIPV4(String strIP) {
		if (strIP == null || "".equals(strIP)) {
			return false;
		}

		int count = 0;
		final String letters = "1234567890.";
		int length = strIP.length();
		for (int i = 0; i < length; i++) {
			char c = strIP.charAt(i);
			if (letters.indexOf(c) == -1) {
				return false;
			}
			if (strIP.charAt(i) == '.') count++;
		}

		if (count != 3) {
			return false;
		}

		String[] parts = new String[4];
		int first = strIP.indexOf(".");
		int last = strIP.lastIndexOf(".");
		parts[0] = strIP.substring(0, first);
		String subip = strIP.substring(0, last);
		int sublength = subip.length();
		int second = subip.lastIndexOf(".");
		parts[1] = subip.substring(first + 1, second);
		parts[2] = subip.substring(second + 1, sublength);
		parts[3] = strIP.substring(last + 1, length);

		for (int i = 0; i < parts.length; i++) {
			if (parts[i] == null || parts[i].equals("")) {
				return false;
			}

			int temp = Integer.parseInt(parts[i]);
			if (temp < 0 || temp > 255) {
				return false;
			}
		}
		
		return true;
	}
    
	public static String unicode2ascii(String str) {
		String[] asciis = str.split("\\\\u");
		String nativeValue = asciis[0];
		try {
			for (int i = 1; i < asciis.length; i++) {
				String code = asciis[i];
				nativeValue += (char) Integer.parseInt(code.substring(0, 4), 16);
				if (code.length() > 4) {
					nativeValue += code.substring(4, code.length());
				}
			}
		} catch (NumberFormatException e) {
			return str;
		}
		return nativeValue;
	}
}

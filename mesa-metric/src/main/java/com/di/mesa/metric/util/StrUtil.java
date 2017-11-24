package com.di.mesa.metric.util;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;

public class StrUtil {
	public StrUtil() {
	}

	public static String utilVersion() {
		return "1.0.0-SNAPSHOT";
	}

	public static String toStr(Object obj) {
		if (obj == null) {
			return "";
		} else {
			return obj.toString();
		}
	}

	public static boolean empty(String s) {
		boolean rtn = false;
		if (s == null || s.length() == 0) {
			rtn = true;
		} else if (s.trim().length() == 0) {
			rtn = true;
		}
		return rtn;
	}

	public static boolean empty(Object s) {
		if (s == null || s.toString().trim().equals("")) {
			return true;
		}
		return false;
	}

	public static int parseInt(String s) {
		return parseInt(s, 0);
	}

	public static int ceilDivide(long a, long b) {
		long c = a % b;
		return (int) (a / b + (c > 0 ? 1 : 0));
	}

	public static String join(Collection s, String delimiter) {
		StringBuffer buffer = new StringBuffer();
		Iterator iter = s.iterator();
		while (iter.hasNext()) {
			buffer.append(iter.next());
			if (iter.hasNext()) {
				buffer.append(delimiter);
			}
		}
		return buffer.toString();
	}

	public static int parseInt(String s, int iDefault) {
		if (s == null || s.equals(""))
			return iDefault;
		if (s.equals("true"))
			return 1;
		if (s.equals("false"))
			return 0;
		try {
			s = s.replaceAll(",", "");
			int l = s.indexOf(".");
			if (l > 0)
				s = s.substring(0, l);
			return Integer.parseInt(s);
		} catch (Exception e) {
			return iDefault;
		}
	}

	public static long parseLong(String s) {
		return parseLong(s, 0L);
	}

	public static long parseLong(String s, long iDefault) {
		if (s == null || s.equals(""))
			return iDefault;
		try {
			s = s.replaceAll(",", "");
			int l = s.indexOf(".");
			if (l > 0)
				s = s.substring(0, l);
			return Long.parseLong(s);
		} catch (Exception e) {
			return iDefault;
		}
	}

	public static int len(String src) {
		int len = 0;
		try {
			byte[] bb = src.getBytes("GBK");
			len = bb.length;
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return len;
	}

	private static String hexString = "0123456789ABCDEF ";

	public static String encode(String str) {
		byte[] bytes = str.getBytes();
		StringBuilder sb = new StringBuilder(bytes.length * 2);
		for (int i = 0; i < bytes.length; i++) {
			sb.append(hexString.charAt((bytes[i] & 0xf0) >> 4));
			sb.append(hexString.charAt((bytes[i] & 0x0f) >> 0));
		}
		return sb.toString();
	}

	public static String decode(String bytes, String code) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream(bytes.length() / 2);
		for (int i = 0; i < bytes.length(); i += 2)
			baos.write((hexString.indexOf(bytes.charAt(i)) << 4 | hexString.indexOf(bytes.charAt(i + 1))));
		String s = "";
		try {
			s = new String(baos.toByteArray(), code);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return s;
	}

	public static String date2String(String pattern, Date date) {
		String retStr = "";
		java.text.SimpleDateFormat ff = new java.text.SimpleDateFormat();
		ff.applyPattern(pattern);
		retStr = ff.format(date);
		return retStr;
	}

	public static String string2Date(String pattern, Date date) {
		java.text.SimpleDateFormat ff = new java.text.SimpleDateFormat();
		ff.applyPattern(pattern);
		String format = ff.format(date);
		return format;
	}

	public static Date string2Date(String pattern, String str) {
		Date date = new Date();
		if (empty(str)) {
			return date;
		}
		java.text.SimpleDateFormat ff = new java.text.SimpleDateFormat();
		ff.applyPattern(pattern);
		try {
			date = ff.parse(str);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return date;
	}

	public static boolean isWeekend(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		int dd = calendar.get(calendar.DAY_OF_WEEK) - 1;
		if (dd == 6 || dd == 0) {
			return true;
		}
		return false;
	}

	public static Float parseFloat(String s) {
		return parseFloat(s, 0.0f);
	}

	public static Float parseFloat(String s, Float defaultValue) {
		if (s == null || s == "") {
			return defaultValue;
		}
		try {
			return Float.parseFloat(s);
		} catch (Exception e) {
			return defaultValue;
		}
	}

	/**
	 * 首字母置为大写
	 * 
	 * @param str
	 * @return
	 * @throws Exception
	 */
	public static String firstLetterToUpper(String str) {
		char[] chars = new char[1];
		chars[0] = str.charAt(0);
		String temp = new String(chars);
		if (chars[0] >= 'A' && chars[0] <= 'Z')
			return str;
		else
			return str.replace(str.charAt(0), (char) (str.charAt(0) - 32));
	}

	/**
	 * 首字母置为小写
	 * 
	 * @param str
	 * @return
	 * @throws Exception
	 */
	public static String firstLetterToLower(String str) {
		char[] chars = new char[1];
		chars[0] = str.charAt(0);
		String temp = new String(chars);
		if (chars[0] >= 'a' && chars[0] <= 'z')
			return str;
		else
			return str.replace(str.charAt(0), (char) (str.charAt(0) + 32));
	}

}
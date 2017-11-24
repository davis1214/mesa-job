package com.di.mesa.metric.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.UUID;

public class PID {

	private static final int AuthCodeKey = 0x7a5c0d68;
	private static final SimpleDateFormat sdf = new SimpleDateFormat("HHmmss");

	private final static String getMD5String(String msg) {
		MessageDigest md;
		try {
			md = MessageDigest.getInstance("MD5");
			md.update(msg.getBytes());
			byte[] byteDigest = md.digest();
			return String.valueOf(byteDigest);
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
			return msg;
		}

	}

	/**
	 * 随机生成32位UUID.
	 * 
	 * @return PID
	 */
	public static String getUUID() {
		String uuid = UUID.randomUUID().toString();
		return uuid.replace("-", "");
	}

	/**
	 * 获取指定num位数的随机数字符串，首位不为0
	 * @param num
	 * @return
	 */
	private final static String random(int num) {
		StringBuffer sb = new StringBuffer();

		Random rand = new Random();
		for (int i = 0; i < num; i++) {
			int n = rand.nextInt(10);
			while (i == 0 && n == 0) {//首位不为0
				n = rand.nextInt(10);
			}
			sb.append(n);
		}
		return sb.toString();
	}

	public static String newValidateCode() {
		return random(6);
	}

	public static String getLname() {
		return sdf.format(new Date());
	}

	public static String getHeadPwd() {
		char c = 'a';
		c = (char) (c + (int) (Math.random() * 26));

		char d = 'a';
		d = (char) (d + (int) (Math.random() * 26));

		String res = String.valueOf(c) + String.valueOf(d);
		return res;

	}

	/*
	 * MD5加密
	 */
	private static final char HEX_DIGITS[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd',
			'e', 'f' };

	public static String toHexString(byte[] b) {
		// String to byte
		StringBuilder sb = new StringBuilder(b.length * 2);
		for (int i = 0; i < b.length; i++) {
			sb.append(HEX_DIGITS[(b[i] & 0xf0) >>> 4]);
			sb.append(HEX_DIGITS[b[i] & 0x0f]);
		}
		return sb.toString();
	}

	/*
	 * MD5加密
	 */
	public static String getMD5Key(String s) {
		try {
			// Create MD5 Hash
			MessageDigest digest = MessageDigest.getInstance("MD5");
			digest.update(s.getBytes());
			byte messageDigest[] = digest.digest();

			return toHexString(messageDigest);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		return "";
	}

}

package com.di.mesa.metric.common;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class StringPlit {

	@Test
	public void testAA() {
		// TODO Auto-generated method stub

		double a = 11;
		double b = 0;

		System.out.println(a / b * 100);
	}

	@Test
	public void genMutipleDimension() {
		String mutiple1 = "module*url";
		String mutiple2 = "successed/total";
		String mutiple3 = "successed+total";
		String mutiple4 = "successed-total";

		List<String> keys = new ArrayList<>();

		StringBuffer buffer = new StringBuffer();
		for (int i = 0; i < mutiple2.length(); i++) {
			char letter = mutiple2.charAt(i);

			if (letter != '/' || letter != '*' || letter != '+' || letter != '-') {
				buffer.append(letter);
			} else {
				keys.add(buffer.toString());
				// System.out.println("buffer.length():" + buffer.length() +
				// " , buffer.capacity():" + buffer.capacity());
				buffer.delete(0, buffer.length());
			}
		}

		if (buffer.length() > 0) {
			keys.add(buffer.toString());
			buffer.delete(0, buffer.length());
		}

		System.out.println("result -> " + keys);
	}

	@Test
	public void genMutipleDimension_all() {
		String mutiple1 = "module*url";
		String mutiple2 = "successed/total";
		String mutiple3 = "successed+total";
		String mutiple4 = "successed-total";

		String mutiple5 = "successed / total";
		String mutiple6 = "successed +total";
		String mutiple7 = "successed+  total";

		String mutiple8 = "(successed_a + successed_b ) /total";

		String[] all = new String[] { mutiple1, mutiple2, mutiple3, mutiple4, mutiple5, mutiple6, mutiple7, mutiple8 };

		// TODO 增加提示信息，请使用半角
		for (String mutipl : all) {

			List<String> keys = new ArrayList<>();

			StringBuffer buffer = new StringBuffer();
			for (int i = 0; i < mutipl.length(); i++) {
				char letter = mutipl.charAt(i);

				if (letter != '/' && letter != '*' && letter != '+' && letter != '-' && letter != '(' && letter != ')') {
					buffer.append(letter);
				} else if (buffer.toString().trim().length() > 0) {
					keys.add(buffer.toString().trim());
					buffer.delete(0, buffer.length());
				}
			}

			if (buffer.toString().trim().length() > 0) {
				keys.add(buffer.toString().trim());
				buffer.delete(0, buffer.length());
			}

			System.out.println(mutipl + " -> result -> " + keys);
		}

	}

	@Test
	public void testSplit() {
		String ss = "pay_promotion.80_localip_10.2.16.160";

		System.out.println(ss.substring(ss.indexOf("localip") + "localip".length() + 1));

		String currentUrl = "http://10.1.8.22:4242/api/query/?summary=true";
		System.out.println(currentUrl.substring(0, currentUrl.indexOf("/", 7)));

		System.out.println(currentUrl.indexOf("/", 8));

		// this.fieldValue =
		// fieldValue.substring(fieldValue.indexOf(computeField) +
		// computeField.length() + 1);

		String a = "udc_157_module_file";

	}

	@Test
	public void testPrettyPrint() {
		StringBuffer buffer = new StringBuffer();

		buffer.append("|").append("app_udc_account,www_ynflmrx9e52e701c527e_udc_account_wf").append("\t").append("|")
				.append("32442").append("\t").append("|").append("\n");

		buffer.append("|").append("app_udc_account").append("\t").append("|").append("442").append("\t").append("|")
				.append("\n");

		buffer.append("|").append("www_ynflmrx9e52e701c527e_udc_account_wf").append("\t").append("|").append("12")
				.append("\t").append("|").append("\n");

		buffer.append("|").append("app_udc_account,www_trace_log").append("\t").append("|").append("64").append("\t")
				.append("|").append("\n");
		System.out.println(buffer.toString());

	}

}

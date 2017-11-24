package com.di.mesa.metric.common;

import java.net.URLEncoder;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.Test;

public class ConcurrentHashMapTest {

	@Test
	public void tst1() {
		ConcurrentHashMap<String, Integer> failedCall = new ConcurrentHashMap<>();

		failedCall.put("url-001", 0);
		failedCall.put("url-002", 0);
		failedCall.put("url-003", 0);

		failedCall.put("url-003", failedCall.get("url-003") + 1);
		failedCall.put("url-001", failedCall.get("url-001") + 8);

		failedCall.remove("url-002");

		System.out.println(failedCall);
		
		System.out.println("http://10.8.96.120:4242/api/query/?summary=true%26start=1477391371%26end=1477391491%26m=sum:ISHOPPING.65{table=ISHOPPING.65_table_credit_info}".charAt(104));

		
		System.out.println(URLEncoder.encode("{"));
		System.out.println(URLEncoder.encode("}"));
		
		String string = "ISHOPPING.65_table_credit_info";
		string = string.substring(string.indexOf("_")+1);
		string = string.substring(string.indexOf("_")+1);
		System.out.println(string);
	}

}

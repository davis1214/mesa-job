package com.di.mesa.metric.util;

import org.junit.Test;

public class UnicodeTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	@Test
	public void unicodeDemo() {

		String str = "\u5fd9\u788c";

		System.out.println("->" + new String(str));

		
		
		//页面上默认增加了转义
		str = "\\u5fd9\\u788c";
		System.out.println("->" + new String(str));

	}

}

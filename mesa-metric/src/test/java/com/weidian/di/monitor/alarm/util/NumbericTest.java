package com.di.mesa.metric.util;

import org.junit.Test;

public class NumbericTest {

	@Test
	public void getPeriod() {
		int period = 240;
		double aa = (double) Math.round(period / 60);
		System.out.println(aa);

		// 4.0m

		period = 249;
		int bb = (int) Math.round(period / 60);
		System.out.println(bb);
		

		
		
	}

}

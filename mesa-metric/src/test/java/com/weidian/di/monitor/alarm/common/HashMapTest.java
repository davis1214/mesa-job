package com.di.mesa.metric.common;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class HashMapTest {

	
	@Test
	public void testMap() {
		// TODO Auto-generated method stub
		Map<String, Double> result = new HashMap<>();
		
		result.put("0112", (double) 1l);
		result.put("0130", (double) 2l);
		
		
		System.out.println(result.get("121"));
		
		if(result.containsKey("test")){
			double a = result.get("test");
		}
		
	}
	
}

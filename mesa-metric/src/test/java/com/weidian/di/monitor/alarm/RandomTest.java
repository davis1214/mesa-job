package com.di.mesa.metric.alarm;

import java.util.Random;

public class RandomTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Random random = new Random(10);
		for (int i = 0; i < 10; i++) {
			System.out.println(random.nextInt(15));
		}

		
		System.out.println("[DI-Monitor]监控项目${index} ${time}发生告警,监控指标${threshold}".replace("${time}", "tetasetss"));
	}

}

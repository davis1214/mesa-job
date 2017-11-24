package com.di.mesa.metric.alarm;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ScheduledExecutorTest {

	private ScheduledExecutorService scheduExec;
	public long start;

	ScheduledExecutorTest() {
		this.scheduExec = Executors.newScheduledThreadPool(4);
		this.start = System.currentTimeMillis();

		timer = new Timer();
		timer.schedule(new TimerTaskTest03(), 1000, 2000);
	}

	public void timerOne() {
		scheduExec.scheduleAtFixedRate(new Runnable() {
			public void run() {
				System.out.println("timerOne invoked .....");
				// throw new RuntimeException();
			}
		}, 1000, 500, TimeUnit.MILLISECONDS);

	}

	public void timerTwo() {
		scheduExec.scheduleAtFixedRate(new Runnable() {
			public void run() {
				System.out.println("timerTwo invoked .....");
			}
		}, 2000, 500, TimeUnit.MILLISECONDS);
	}

	public static void main(String[] args) {
		ScheduledExecutorTest test = new ScheduledExecutorTest();
		test.timerOne();
		test.timerTwo();
	}

	Timer timer;

}

class TimerTaskTest03 extends TimerTask {
	@Override
	public void run() {
		Date date = new Date(this.scheduledExecutionTime());
		System.out.println("本次执行该线程的时间为：" + date);
	}
}
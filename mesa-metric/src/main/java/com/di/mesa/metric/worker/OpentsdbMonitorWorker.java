package com.di.mesa.metric.worker;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.di.mesa.metric.client.ShuffledOpentsdbClient;

public class OpentsdbMonitorWorker extends Thread {

	private static final Logger logger = LoggerFactory.getLogger(OpentsdbMonitorWorker.class);

	private long sleep_time_ms = 2000l;
	private int monitor_fail_time = 3;
	private int batch_check_time = 30000;
	private ShuffledOpentsdbClient opentsdbDao;

	public OpentsdbMonitorWorker(ShuffledOpentsdbClient opentsdbDao) {
		this.opentsdbDao = opentsdbDao;
	}

	@Override
	public void run() {
		long failCounter = 0l;
		long lastCheckTime = System.currentTimeMillis();

		boolean shouldImmediatelyCheck = false;
		boolean result = true;

		while (true) {
			try {
				// TODO 连续3次请求失败 or 60s检查一次
				if ((System.currentTimeMillis() - lastCheckTime > batch_check_time) || failCounter > 0
						|| shouldImmediatelyCheck) {
					lastCheckTime = System.currentTimeMillis();

					try {
						result = opentsdbDao.checkService();
					} catch (IOException e) {
						result = false;
						e.printStackTrace();
						logger.error(e.getMessage(), e);
					}

					if (!result) {
						failCounter++;
						logger.info("error " + failCounter + ", " + opentsdbDao.getCurrentUrl());
					} else {
						failCounter = 0l;
						shouldImmediatelyCheck = false;
						logger.info("success " + ", " + opentsdbDao.getCurrentUrl());
					}
				}

				if (failCounter > monitor_fail_time) {
					opentsdbDao.shuffleClientAutoly();
					failCounter = 0l;
					shouldImmediatelyCheck = true;
				} else {
					Thread.sleep(sleep_time_ms);
				}
			} catch (InterruptedException e) {
				logger.error(e.getMessage(), e);
			} catch (Exception e) {
				logger.error("monitor thread batch proc error", e);
			}
		}
	}
}

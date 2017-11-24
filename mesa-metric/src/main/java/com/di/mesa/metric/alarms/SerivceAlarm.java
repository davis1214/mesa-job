package com.di.mesa.metric.alarms;

import java.io.IOException;
import java.net.URI;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;

import com.di.mesa.metric.common.AlarmConstants;
import com.di.mesa.metric.model.MAlarmCondition;
import com.di.mesa.metric.util.Unicode2Utf8;
import com.di.mesa.metric.client.util.ExecutorApiClient;
import com.di.mesa.metric.model.MAlarm;
import jregex.Matcher;
import jregex.Pattern;


/**
 * 接口探活告警状态检查
 *
 * @param
 * @throws IOException
 */
public class SerivceAlarm extends Alarm {


    public SerivceAlarm() {
        super();
    }

    @Override
    public int execute(MAlarm malarm) throws Exception {
        MAlarmCondition malarmCondition = malarm.getMalarmConditions().get(0);
        ExecutorApiClient apiclient = ExecutorApiClient.getInstance();

        URI uri = URI.create(malarm.getAlarmMetric().trim());
        String response = null;
        try {
            response = apiclient.httpGet(uri, null);
        } catch (IOException e) {
            logger.error(e.getMessage() + " " + malarm.getAlarmMetric(), e);
        }

        // TODO 没有数据，也认为是异常的 细化下策略. 如果无法访访问，则认为正常？
        boolean successed = stateCheck(response, malarmCondition.getThreshold(), malarm.getAlarmMetric());
        if (!successed) {
            logger.info("token jobid {} ,interface com.di.mesa.metric.alarm [{}] check failed", malarm.getmAlarmId(),
                    malarm.getAlarmMetric());
            AlarmStateRetryChecker stateRetryChecker = new AlarmStateRetryChecker(malarm.getAlarmMetric().trim(), malarm);
            if (malarm.getAlarmTriggerCountInInteger() > 1) {
                stateRetryChecker.start();
            } else if (malarm.getAlarmTriggerCountInInteger() == 1) {
                stateRetryChecker
                        .alarm(malarmCondition.getThreshold(), malarmCondition.getmAlarmConditionsId(), malarm);
            }
        } else {
            logger.info("token jobid {} ,interface com.di.mesa.metric.alarm {} check successed", malarm.getmAlarmId(),
                    malarm.getAlarmMetric());
        }

        return AlarmConstants.DATA_FOUND;

    }

    private boolean stateCheck(String response, String threshold, String alarmIndex) {
        if (response == null || response.isEmpty()) {
            logger.debug("response content is null for interface com.di.mesa.metric.alarm {}", alarmIndex);
            return false;
        }

        try {
            // .*Hi.*?
            // threadhold = ".*\"status_code\":0,.*?"; //
            // ({file}/itemd\.log&ext=&data=).*?
            Pattern pattern = new Pattern(threshold);
            Matcher matcher = pattern.matcher(Unicode2Utf8.unicodeToUtf8(response));
            return matcher.matches();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return false;
    }

    private class AlarmStateRetryChecker extends Thread {
        private MAlarm malarm;
        private String url;
        private String threshold;
        private int triggerCount;
        private long alarmConditionsId;

        public AlarmStateRetryChecker(String url, MAlarm malarm) {
            this.malarm = malarm;
            this.url = url;
            this.triggerCount = 1;
            MAlarmCondition mAlarmConditions = malarm.getMalarmConditions().get(0);
            this.threshold = mAlarmConditions.getThreshold();
            this.alarmConditionsId = mAlarmConditions.getmAlarmConditionsId();
        }

        @Override
        public void run() {
            AtomicBoolean shouldStop = new AtomicBoolean(false);
            ExecutorApiClient apiclient = ExecutorApiClient.getInstance();
            URI uri = URI.create(url);

            while (!shouldStop.get()) {
                if (this.triggerCount <= malarm.getAlarmTriggerCountInInteger()) {
                    String response = null;
                    try {
                        response = apiclient.httpGet(uri, null);
                    } catch (IOException e) {
                        logger.error("http get method failed -> " + e.getMessage());
                    }

                    boolean successed = stateCheck(response, threshold, malarm.getAlarmMetric());
                    if (!successed) {
                        this.triggerCount++;
                    } else {
                        shouldStop.set(true);
                        this.triggerCount = 0;
                    }
                } else {
                    shouldStop.set(true);
                }

                try {
                    Thread.sleep(5000l);// sleep 5s
                } catch (InterruptedException e) {
                }
            }

            if (this.triggerCount > 0) {
                try {
                    alarm(threshold, alarmConditionsId, malarm);
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }

        public void alarm(String threshold, long alarmConditionsId, MAlarm malarm) throws Exception {
            try {
                String content = malarm.getTemplate().replace("${time}", simpleDateFormat.format(new Date()))
                        .replace("${index}", malarm.getAlarmMetric()).replace("${threshold}", threshold);

                logger.info("token jobid {} ,com.di.mesa.metric.alarm content {}", malarm.getmAlarmId(), content);
                handleAlarm(malarm, content);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }


}

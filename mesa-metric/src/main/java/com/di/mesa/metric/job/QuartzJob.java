package com.di.mesa.metric.job;

import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

import com.di.mesa.metric.common.AlarmConstants;
import com.di.mesa.metric.manager.AlarmManager;
import com.di.mesa.metric.util.PropertyUtil;
import com.di.mesa.metric.model.MAlarm;
import com.di.mesa.metric.util.DateUtil;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QuartzJob implements Job {
    public final Logger logger = LoggerFactory.getLogger(QuartzJob.class);

    private int minDelayTime; // 160 seconds
    private int maxDelayTime; // 600 seconds
    private int middleDelayTime; // 600 seconds

    private AlarmManager alarmManager;
    private AtomicLong cost;

    public QuartzJob() {
        logger.info(QuartzJob.class.getName() + " init " + Thread.currentThread().getName());
        minDelayTime = PropertyUtil.getIntegerValueByKey("alarm.properties", "alarm.check.min.delay.time.in.seconds");
        maxDelayTime = PropertyUtil.getIntegerValueByKey("alarm.properties", "alarm.check.max.delay.time.in.seconds");
        middleDelayTime = PropertyUtil.getIntegerValueByKey("alarm.properties", "alarm.check.middle.delay.time.in.seconds");

        String urls = PropertyUtil.getValueByKey("alarm.properties", "opentsdb.query.url");
        alarmManager = new AlarmManager(urls);
        cost = new AtomicLong(System.currentTimeMillis());
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        try {
            long start = System.currentTimeMillis();
            MAlarm mAlarm = (MAlarm) context.getMergedJobDataMap().get(AlarmConstants.ALARM_APP_INSTANCE);

            /**
             * 1: alarm
             * 0: not alarm
             */
            if (mAlarm.getIsAlarm() == 0) {
                Scheduler scheduler = context.getScheduler();
                JobKey jobKey = JobKey.jobKey(mAlarm.getmAlarmId(), mAlarm.getAlarmMetric());
                scheduler.pauseJob(jobKey);
                logger.info("token jobid {} , alarm turn off ,description " + mAlarm.getDescription() + " ,cost {}", mAlarm.getmAlarmId(), (System.currentTimeMillis() - cost.get()));
                return;
            }

            if (shouldDelay(mAlarm)) {
                //reset the last window process
                long minDelayTime = PropertyUtil.getIntegerValueByKey("alarm.properties", "alarm.check.min.delay.time.in.seconds");
                long end = DateUtil.getCurTimestampInMinutes() / 1000 - minDelayTime;
                mAlarm.setEnd(end - mAlarm.getAlarmFrequency());
                mAlarm.setStart(mAlarm.getEnd() - mAlarm.getAlarmFrequency());

                logger.debug("token jobid {} should reset start-end {}", mAlarm.getmAlarmId(),
                        new Date(mAlarm.getStart() * 1000) + "-" + new Date(mAlarm.getEnd() * 1000));
            }

            cost.set(System.currentTimeMillis());

            logger.info("token jobid {} process Getting MAlarm,cost {}", mAlarm.getmAlarmId(), (System.currentTimeMillis() - cost.get()));
            cost.set(System.currentTimeMillis());

            startQuartzJob(mAlarm);
            logger.info("token jobid {} process Quartz Job,cost {}", mAlarm.getmAlarmId(), (System.currentTimeMillis() - cost.get()));

            resetAlarmWindowBatchTime(mAlarm);
            logger.info("token jobid {} processed ,cost {}", mAlarm.getmAlarmId(), (System.currentTimeMillis() - start));
        } catch (Exception e) {
            //e.printStackTrace();
            logger.error(e.getMessage(), e);
        }

    }

    // TODO区分没有数据还是没有数据生成,会有延迟
    private void startQuartzJob(MAlarm mAlarm) throws Exception,
            SchedulerException {
        logger.info("token jobid {} ,mesa.metric.alarm name {} start to check", mAlarm.getmAlarmId(), mAlarm.getAlarmMetric());
        mAlarm.setStart(mAlarm.getEnd());
        mAlarm.setEnd(mAlarm.getStart() + mAlarm.getAlarmFrequency());

        int result = alarmManager.execute(mAlarm);
        if (result > AlarmConstants.ZERO_DATA_FOUND || result == AlarmConstants.NO_DATA_FOUND) {
            logger.info("token jobid {} , result code found {}", mAlarm.getmAlarmId(), result);
            //scheduleJob.setStart(scheduleJob.getEnd());
            //scheduleJob.setEnd(scheduleJob.getStart() + scheduleJob.getAlarmFrequency());
        } else {
            logger.error("token jobid {} , unexpected result code found {}", mAlarm.getmAlarmId(), result);
        }
    }

    private void resetAlarmWindowBatchTime(MAlarm mAlarm) {
        if (shouldResetDataAlarmWinBatchTime(mAlarm)) {
            mAlarm.setEnd(DateUtil.getCurTimestampInMinutes() / 1000);
            mAlarm.setStart(mAlarm.getEnd() - mAlarm.getAlarmFrequency());

            String rawTime = mAlarm.getStart() + "-" + mAlarm.getEnd();
            logger.info("token jobid {} , mesa.metric.alarm batch win time should reset {} ", mAlarm.getmAlarmId(), rawTime);

            try {
                while (!shouldDelay(mAlarm)) {
                    int result = alarmManager.execute(mAlarm);
                    logger.info("token jobid {} , result code found in resetAlarmWindowBatchTime {}", mAlarm.getmAlarmId(), result);
                    mAlarm.setStart(mAlarm.getEnd());
                    mAlarm.setEnd(mAlarm.getStart() + mAlarm.getAlarmFrequency());
                }

            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    private boolean shouldResetDataAlarmWinBatchTime(MAlarm mAlarm) {
        long time = DateUtil.getCurTimestamp() / 1000;
        return mAlarm.getAlarmType() == AlarmConstants.ALARM_TYPE_DATA && ((time - mAlarm.getEnd()) > maxDelayTime);
    }

    private boolean shouldDelay(MAlarm mAlarm) {
        long time = DateUtil.getCurTimestamp() / 1000;

        if (mAlarm.getAlarmType() == AlarmConstants.ALARM_TYPE_DATA && (time - mAlarm.getEnd() < minDelayTime)) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("[jobname ").append(mAlarm.getAlarmMetric()).append(" ,start time ")
                    .append(new Date(time * 1000)).append(" ,end time ").append(new Date(mAlarm.getEnd() * 1000))
                    .append("]");
            logger.info("token jobid {} ,job {} would delay", mAlarm.getmAlarmId(), buffer.toString());
            return true;
        }

        return false;
    }

}
package com.di.mesa.metric.alarms;

import com.di.mesa.metric.job.MsgJob;
import com.di.mesa.metric.model.MAlarm;

public class TopicAlarm extends Alarm {

    public TopicAlarm() {
        super();
    }

    @Override
    public int execute(MAlarm malarm) throws Exception {
        logger.info("token jobid {} ,ALARM_TYPE_TOPIC", malarm.getmAlarmId());
        return 0;
    }

    @Override
    public int handleAlarm(MAlarm malarm, String content) throws Exception {
        return 0;
    }

    @Override
    public Alarm addAlarmReceiver(MsgJob msgJob) {
        return null;
    }

}

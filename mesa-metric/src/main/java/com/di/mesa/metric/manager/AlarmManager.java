package com.di.mesa.metric.manager;

import com.di.mesa.metric.alarms.Alarm;
import com.di.mesa.metric.alarms.DataAlarm;
import com.di.mesa.metric.alarms.SerivceAlarm;
import com.di.mesa.metric.alarms.TopicAlarm;
import com.di.mesa.metric.common.AlarmConstants;
import com.di.mesa.metric.job.PhoneMsgJob;
import com.di.mesa.metric.job.WeixinMsgJob;
import com.di.mesa.metric.model.MAlarm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlarmManager {
	public final Logger logger = LoggerFactory.getLogger(AlarmManager.class);

	private String opentsdbUrls;

	private AlarmBuilder builder;

	public AlarmManager(String opentsdbUrls) {
		this.opentsdbUrls = opentsdbUrls;
	}

	public int execute(MAlarm malarm) throws Exception {
		builder = new AlarmBuilder(malarm.getAlarmType());

		Alarm alarm = builder.build();

		if (malarm.getAlarmConcerner().needAlarm()) {
			if (malarm.getAlarmConcerner().getTellers().size() > 0) {
				alarm.addAlarmReceiver(new PhoneMsgJob());
			}
			if (malarm.getAlarmConcerner().getWeixinTeller().size() > 0) {
				alarm.addAlarmReceiver(new WeixinMsgJob());
			}
		} else {
			logger.info("token jobid {} ,com.di.mesa.metric.alarm concerner is null.No needs to send com.di.mesa.metric.alarm msg", malarm.getmAlarmId());
		}
		return alarm.execute(malarm);
	}

	private class AlarmBuilder {
		private int alarmType;

		public AlarmBuilder(int alarmType) {
			super();
			this.alarmType = alarmType;
		}

		private Alarm build() {
			Alarm alarm = null;
			switch (this.alarmType) {
			case AlarmConstants.ALARM_TYPE_TOPIC:
				alarm = new TopicAlarm();
				break;
			case AlarmConstants.ALARM_TYPE_DATA:
				alarm = new DataAlarm(opentsdbUrls);
				break;
			case AlarmConstants.ALARM_TYPE_STATE:
				alarm = new SerivceAlarm();
				break;
			default:
				break;
			}
			return alarm;
		}
	}

}

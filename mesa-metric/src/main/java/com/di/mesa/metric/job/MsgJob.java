package com.di.mesa.metric.job;

import com.di.mesa.metric.model.AlarmConcerner;

import java.io.IOException;

public interface MsgJob {
	
	public void sendMsg(final AlarmConcerner alarmConcerners, String content) throws IOException;
	
}

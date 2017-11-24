package com.di.mesa.metric;

public interface AlarmServerMBean {

    public boolean getStopFlag();

    public String getServerState();

    public void shutdown();

}

package com.di.mesa.metric.model;

import java.io.IOException;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class ExecutorInfo implements java.io.Serializable, Comparable<ExecutorInfo> {
    private static final long serialVersionUID = 3009746603773371263L;

    private double remainingMemoryPercent;
    private long remainingMemoryInMB;
    private double cpuUsage;

    private int remainingAlarmCapacity;
    private int numberOfAssignedAlarms;

    private long lastDispatchedTime;

    @Override
    public int compareTo(ExecutorInfo o) {
        return null == o ? 1 : this.hashCode() - o.hashCode();
    }

    public double getCpuUsage() {
        return this.cpuUsage;
    }

    public void setCpuUpsage(double value) {
        this.cpuUsage = value;
    }

    public double getRemainingMemoryPercent() {
        return this.remainingMemoryPercent;
    }

    public void setRemainingMemoryPercent(double value) {
        this.remainingMemoryPercent = value;
    }

    public long getRemainingMemoryInMB() {
        return this.remainingMemoryInMB;
    }

    public void setRemainingMemoryInMB(long value) {
        this.remainingMemoryInMB = value;
    }

    public int getRemainingAlarmCapacity() {
        return this.remainingAlarmCapacity;
    }

    public void setRemainingAlarmCapacity(int value) {
        this.remainingAlarmCapacity = value;
    }

    public long getLastDispatchedTime() {
        return this.lastDispatchedTime;
    }

    public void setLastDispatchedTime(long value) {
        this.lastDispatchedTime = value;
    }

    public int getNumberOfAssignedAlarms() {
        return this.numberOfAssignedAlarms;
    }

    public void setNumberOfAssignedAlarms(int value) {
        this.numberOfAssignedAlarms = value;
    }

    public ExecutorInfo() {
    }

    public ExecutorInfo(double remainingMemoryPercent, long remainingMemory, int remainingAlarmCapacity,
                        long lastDispatched, double cpuUsage, int numberOfAssignedFlows) {
        this.remainingMemoryInMB = remainingMemory;
        this.cpuUsage = cpuUsage;
        this.remainingAlarmCapacity = remainingAlarmCapacity;
        this.remainingMemoryPercent = remainingMemoryPercent;
        this.lastDispatchedTime = lastDispatched;
        this.numberOfAssignedAlarms = numberOfAssignedFlows;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ExecutorInfo) {
            boolean result = true;
            ExecutorInfo stat = (ExecutorInfo) obj;

            result &= this.remainingMemoryInMB == stat.remainingMemoryInMB;
            result &= this.cpuUsage == stat.cpuUsage;
            result &= this.remainingAlarmCapacity == stat.remainingAlarmCapacity;
            result &= this.remainingMemoryPercent == stat.remainingMemoryPercent;
            result &= this.numberOfAssignedAlarms == stat.numberOfAssignedAlarms;
            result &= this.lastDispatchedTime == stat.lastDispatchedTime;
            return result;
        }
        return false;
    }

    @Override
    public String toString() {
        return "ExecutorInfo [remainingMemoryPercent=" + remainingMemoryPercent + ", remainingMemoryInMB="
                + remainingMemoryInMB + ", cpuUsage=" + cpuUsage + ", remainingAlarmCapacity=" + remainingAlarmCapacity
                + ", numberOfAssignedAlarms=" + numberOfAssignedAlarms + ", lastDispatchedTime=" + lastDispatchedTime
                + "]";
    }

    public static ExecutorInfo fromJSONString(String jsonString) throws JsonParseException, JsonMappingException,
            IOException {
        if (null == jsonString || jsonString.length() == 0)
            return null;
        return new ObjectMapper().readValue(jsonString, ExecutorInfo.class);
    }
}

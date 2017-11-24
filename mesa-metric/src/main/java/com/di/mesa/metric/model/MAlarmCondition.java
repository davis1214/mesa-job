package com.di.mesa.metric.model;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import java.util.Date;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MAlarmCondition {
    private long mAlarmConditionsId;
    private String mAlarmid;

    private int type;
    private int period;
    private int slideType;
    private String threshold;
    private String mapComputeKey;

    private int periodBak;

    private Date createTime;
    private Date updateTime;

    /**
     * @return the periodBak
     */
    public int getPeriodBak() {
        return periodBak;
    }

    /**
     * @param periodBak the periodBak to set
     */
    public void setPeriodBak(int periodBak) {
        this.periodBak = periodBak;
    }

    public String getMapComputeKey() {
        return mapComputeKey;
    }

    public void setMapComputeKey(String mapComputeKey) {
        this.mapComputeKey = mapComputeKey;
    }


    public long getmAlarmConditionsId() {
        return mAlarmConditionsId;
    }

    public void setmAlarmConditionsId(long mAlarmConditionsId) {
        this.mAlarmConditionsId = mAlarmConditionsId;
    }

    public String getmAlarmid() {
        return mAlarmid;
    }

    public void setmAlarmid(String mAlarmid) {
        this.mAlarmid = mAlarmid;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getPeriod() {
        return period;
    }

    public void setPeriod(int period) {
        this.period = period;
    }

    public int getSlideType() {
        return slideType;
    }

    public void setSlideType(int slideType) {
        this.slideType = slideType;
    }

    public String getThreshold() {
        return threshold;
    }

    public double getThresholdDoubleType() {
        if (this.threshold == null || this.threshold.length() == 0) {
            return 0;
        }
        return Double.valueOf(threshold);
    }

    public void setThreshold(String threshold) {
        this.threshold = threshold;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    @Override
    public String toString() {
        return "MAlarmCondition [mAlarmConditionsId=" + mAlarmConditionsId + ", mAlarmid=" + mAlarmid + ", type="
                + type + ", period=" + period + ", slideType=" + slideType + ", threshold=" + threshold
                + ", createTime=" + createTime + ", updateTime=" + updateTime + "]";
    }

}

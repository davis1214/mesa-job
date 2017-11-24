package com.di.mesa.metric.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.di.mesa.metric.common.AlarmConstants;
import com.di.mesa.metric.common.CommonConstant;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MAlarm {

    private String mAlarmId;
    private int alarmType;

    private String alarmMetric;
    private String alarmItemName;
    private long alarmFrequency;
    private int alarmCount;
    private String alarmTriggerCount;

    private String alarmLevel;
    private String connectionType;

    /**
     * 1: alarm
     * 0: not alarm
     */
    private int isAlarm;

    private String departmentId;
    private String userName;
    private int isValid = 1;

    private boolean isMapCompute;
    private long start;
    private long end;
    private long cycleStart;
    private long cycleEnd;


    private int errorCode;
    private String errorMsg;
    private String description;
    private String jobStatus;
    private String cronExpression;

    private String template;

    private Date createTime;
    private Date updateTime;


    private String testText;

    private String topic;

    /**
     * 接口探活只有一个告警条件，数据监控有多个
     */
    private List<MAlarmCondition> malarmConditions;


    /**
     * 一个告警只能对应一个计算纬度
     */
    private ComputeDimension computeDimension;


    private AlarmConcerner alarmConcerner;


    private StringBuffer comparedJudgeBuffer;
    private StringBuffer commonJudgeBuffer;

    private String monitoredValue;

    private List<String> mutipleComputeDimension;


    public int getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public AlarmConcerner getAlarmConcerner() {
        return alarmConcerner;
    }

    public void setAlarmConcerner(AlarmConcerner alarmConcerner) {
        this.alarmConcerner = alarmConcerner;
    }

    public String getCronExpression() {
        return cronExpression;
    }

    public void setCronExpression(String cronExpression) {
        this.cronExpression = cronExpression;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getJobStatus() {
        return jobStatus;
    }

    public void setJobStatus(String jobStatus) {
        this.jobStatus = jobStatus;
    }

    public long getCycleStart() {
        return cycleStart;
    }

    public void setCycleStart(long cycleStart) {
        this.cycleStart = cycleStart;
    }

    public long getCycleEnd() {
        return cycleEnd;
    }

    public void setCycleEnd(long cycleEnd) {
        this.cycleEnd = cycleEnd;
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    /**
     * @return the monitoredValue
     */
    public String getMonitoredValue() {
        return monitoredValue;
    }

    /**
     * @param monitoredValue the monitoredValue to set
     */
    public void setMonitoredValue(String monitoredValue) {
        this.monitoredValue = monitoredValue;
    }

    public String getAlarmRecrod() {
        StringBuffer threshold = new StringBuffer();

        if (getCommonJudge().length() > 0) {
            if (threshold.toString().length() > 0) {
                threshold.append("； ");
            }
            threshold.append(getCommonJudge().substring(1));
        }

        if (getComparedJudge().length() > 0) {
            if (threshold.toString().length() > 0) {
                threshold.append("； ");
            }

            threshold.append("同比：").append(getComparedJudge().substring(1));
        }

        return threshold.toString();
    }


    public String getAlarmValue() {
        StringBuffer threshold = new StringBuffer();
        threshold.append(getMonitoredValue());

        if (getComparedJudge().length() > 0) {
            if (threshold.toString().length() > 0) {
                threshold.append("； ");
            }

            threshold.append("同比：").append(getComparedJudge().substring(1));
        }

        return threshold.toString();
    }

    public String getConditions() {
        StringBuffer conditionsBuffer = new StringBuffer();
        for (MAlarmCondition mAlarmCondition : malarmConditions) {
            switch (mAlarmCondition.getType()) {
                case AlarmConstants.ALARM_TYPE_OLDER:
                    conditionsBuffer.append("；").append(">").append(mAlarmCondition.getThreshold());
                    break;
                case AlarmConstants.ALARM_TYPE_OLDER_EQUAL:
                    conditionsBuffer.append("；").append(">=").append(mAlarmCondition.getThreshold());
                    break;
                case AlarmConstants.ALARM_TYPE_LOWER:
                    conditionsBuffer.append("；").append("<").append(mAlarmCondition.getThreshold());
                    break;
                case AlarmConstants.ALARM_TYPE_LOWER_EQUAL:
                    conditionsBuffer.append("；").append("<=").append(mAlarmCondition.getThreshold());
                    break;
                case AlarmConstants.ALARM_TYPE_OLDER_THAN_LAST:
                    conditionsBuffer.append("；").append("大于上次").append(mAlarmCondition.getThreshold());
                    break;
                case AlarmConstants.ALARM_TYPE_OLDER_EQUAL_THAN_LAST:
                    conditionsBuffer.append("；").append("大于等于上次").append(mAlarmCondition.getThreshold());
                    break;
                case AlarmConstants.ALARM_TYPE_LOWER_THAN_LAST:
                    conditionsBuffer.append("；").append("小于上次").append(mAlarmCondition.getThreshold());
                    break;
                case AlarmConstants.ALARM_TYPE_LOWER_EQUAL_THAN_LAST:
                    conditionsBuffer.append("；").append("小于等于上次").append(mAlarmCondition.getThreshold());
                    break;
                case AlarmConstants.ALARM_TYPE_COMPARED_RATION:
                    switch (mAlarmCondition.getSlideType()) {
                        case AlarmConstants.ALARM_SLIDE_TYPE_UP:
                            conditionsBuffer.append("；").append("同比").append(getPeriod(mAlarmCondition.getPeriod()))
                                    .append("m上升").append(mAlarmCondition.getThreshold()).append("%");
                            break;
                        case AlarmConstants.ALARM_SLIDE_TYPE_DOWN:
                            conditionsBuffer.append("；").append("同比").append(getPeriod(mAlarmCondition.getPeriod()))
                                    .append("m下降").append(mAlarmCondition.getThreshold()).append("%");
                            break;
                        default:
                            conditionsBuffer.append("；").append("同比").append(getPeriod(mAlarmCondition.getPeriod()))
                                    .append("m上下波动").append(mAlarmCondition.getThreshold()).append("%");
                            break;
                    }
                    break;
                case AlarmConstants.ALARM_TYPE_CONTINOUS_COMPARED_RATION:
                    switch (mAlarmCondition.getSlideType()) {
                        case AlarmConstants.ALARM_SLIDE_TYPE_UP:
                            conditionsBuffer.append("；").append("连续").append(getPeriod(mAlarmCondition.getPeriodBak()))
                                    .append("m同比上升").append(mAlarmCondition.getThreshold()).append("%");
                            break;
                        case AlarmConstants.ALARM_SLIDE_TYPE_DOWN:
                            conditionsBuffer.append("；").append("连续").append(getPeriod(mAlarmCondition.getPeriodBak()))
                                    .append("m同比下降").append(mAlarmCondition.getThreshold()).append("%");
                            break;
                        default:
                            conditionsBuffer.append("；").append("连续").append(getPeriod(mAlarmCondition.getPeriodBak()))
                                    .append("m同比上下波动").append(mAlarmCondition.getThreshold()).append("%");
                            break;
                    }
                    break;
                case AlarmConstants.ALARM_TYPE_MAX_VALUE:
                    conditionsBuffer.append("；").append("最大值").append(mAlarmCondition.getThreshold());
                    break;
                case AlarmConstants.ALARM_TYPE_MIN_VALUE:
                    conditionsBuffer.append("；").append("最小值").append(mAlarmCondition.getThreshold());
                    break;
                default:
                    break;
            }
        }

        return conditionsBuffer.toString().substring(1);
    }

    private int getPeriod(int period) {
        return (int) Math.round(period / 60);
    }


    /**
     * @return the commonJudgeBuffer
     */
    public StringBuffer getCommonJudgeBuffer() {
        if (commonJudgeBuffer == null) {
            commonJudgeBuffer = new StringBuffer();
        }
        return commonJudgeBuffer;
    }

    public String getCommonJudge() {
        return getCommonJudgeBuffer().toString();
    }

    /**
     * @param commonJudgeBuffer the commonJudgeBuffer to set
     */
    public void setCommonJudgeBuffer(StringBuffer commonJudgeBuffer) {
        this.commonJudgeBuffer = commonJudgeBuffer;
    }

    public StringBuffer getComparedJudgeBuffer() {
        if (comparedJudgeBuffer == null) {
            comparedJudgeBuffer = new StringBuffer();
        }
        return comparedJudgeBuffer;
    }

    public String getComparedJudge() {
        return getComparedJudgeBuffer().toString();
    }

    /**
     * @param comparedJudgeBuffer the comparedJudgeBuffer to set
     */
    public void setComparedJudgeBuffer(StringBuffer comparedJudgeBuffer) {
        this.comparedJudgeBuffer = comparedJudgeBuffer;
    }

    public String getTemplate() {
        if (StringUtils.isEmpty(template)) {
            template = "[告警]实时流任务[${name}]异常 ,告警时间 ${time}";
        }
        return template;
    }

    public void setTemplate(String template) {
        this.template = template;
    }

    public ComputeDimension getComputeDimension() {
        return computeDimension;
    }

    public void setComputeDimension(ComputeDimension computeDimension) {
        this.computeDimension = computeDimension;
    }


    public List<String> getMutipleComputeDimension() {
        if (mutipleComputeDimension == null) {
            mutipleComputeDimension = new ArrayList<>();

            String computeField = this.getComputeDimension().getComputeField();
            StringBuffer buffer = new StringBuffer();
            for (int i = 0; i < computeField.length(); i++) {
                char letter = computeField.charAt(i);

                if (letter != '/' && letter != '*' && letter != '+' && letter != '-' && letter != '(' && letter != ')') {
                    buffer.append(letter);
                } else if (buffer.toString().trim().length() > 0
                        && containsDimensions.contains(buffer.toString().trim())) {
                    mutipleComputeDimension.add(buffer.toString().trim());
                    buffer.delete(0, buffer.length());
                }
            }

            if (buffer.toString().trim().length() > 0 && containsDimensions.contains(buffer.toString().trim())) {
                mutipleComputeDimension.add(buffer.toString().trim());
                buffer.delete(0, buffer.length());
            }
        }
        return mutipleComputeDimension;
    }

    private List<String> containsDimensions = null;

    public void initMutilComputeDimension(List<ComputeDimension> computeDimensions) {
        containsDimensions = new ArrayList<String>();
        for (ComputeDimension computeDimensions2 : computeDimensions) {
            if (this.getComputeDimension().getComputeField().contains(computeDimensions2.getComputeField())
                    && !this.getComputeDimension().getComputeField().equals(computeDimensions2.getComputeField())) {
                containsDimensions.add(computeDimensions2.getComputeField());
            }
        }
    }

    public List<MAlarmCondition> getMalarmConditions() {
        return malarmConditions;
    }

    public void setMalarmConditions(List<MAlarmCondition> malarmConditions) {
        this.malarmConditions = malarmConditions;
    }

    public int getIsValid() {
        return isValid;
    }

    public void setIsValid(int isValid) {
        this.isValid = isValid;
    }

    public String getTestText() {
        return testText;
    }

    public void setTestText(String testText) {
        this.testText = testText;
    }

    public String getmAlarmId() {
        return mAlarmId;
    }

    public String getmAlarmIdInStringType() {
        return String.valueOf(mAlarmId);
    }

    public void setmAlarmId(String mAlarmId) {
        this.mAlarmId = mAlarmId;
    }

    public int getAlarmType() {
        return alarmType;
    }

    public void setAlarmType(int alarmType) {
        this.alarmType = alarmType;
    }

    public String getAlarmMetric() {
        return alarmMetric;
    }

    public void setAlarmMetric(String alarmMetric) {
        this.alarmMetric = alarmMetric;
    }


    public String getAlarmItemName() {
        return alarmItemName;
    }

    public void setAlarmItemName(String alarmItemName) {
        this.alarmItemName = alarmItemName;
    }

    public long getAlarmFrequency() {
        return alarmFrequency;
    }

    public int getAlarmFrequencyInInteger() {
        return (int) alarmFrequency;
    }

    public String getConnectionType() {
        return connectionType;
    }

    public void setConnectionType(String connectionType) {
        this.connectionType = connectionType;
    }

    public String getTopic() {
        if (StringUtils.isEmpty(topic)) {
            return CommonConstant._MESA;
        }
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setAlarmFrequency(long alarmFrequency) {
        this.alarmFrequency = alarmFrequency;
    }

    public int getAlarmCount() {
        return alarmCount;
    }

    public void setAlarmCount(int alarmCount) {
        this.alarmCount = alarmCount;
    }

    public String getAlarmTriggerCount() {
        return alarmTriggerCount;
    }

    public int getAlarmTriggerCountInInteger() {
        return Integer.valueOf(alarmTriggerCount);
    }

    public void setAlarmTriggerCount(String alarmTriggerCount) {
        this.alarmTriggerCount = alarmTriggerCount;
    }

    public String getAlarmLevel() {
        return alarmLevel;
    }

    public void setAlarmLevel(String alarmLevel) {
        this.alarmLevel = alarmLevel;
    }

    public int getIsAlarm() {
        return isAlarm;
    }

    public void setIsAlarm(int isAlarm) {
        this.isAlarm = isAlarm;
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

    public String getDepartmentId() {
        return departmentId;
    }

    public void setDepartmentId(String departmentId) {
        this.departmentId = departmentId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public void initMapComputeField() {
        this.isMapCompute = getComputeDimension().isMapCompute();
    }

    public boolean isMapCompute() {
        return isMapCompute;
    }

    @Override
    public String toString() {
        return "MAlarm [mAlarmId=" + mAlarmId + ", alarmType=" + alarmType
                + ", alarmMetric=" + alarmMetric + ", alarmItemName=" + alarmItemName
                + ", alarmFrequency=" + alarmFrequency + ", alarmCount=" + alarmCount + ", alarmTriggerCount="
                + alarmTriggerCount + ", alarmLevel=" + alarmLevel + ", testText=" + testText + ", isAlarm=" + isAlarm
                + ", createTime=" + createTime + ", updateTime=" + updateTime + ", departmentId=" + departmentId
                + ", userName=" + userName + ", isValid=" + isValid + "]";
    }


}

package com.di.mesa.metric.alarms;

import com.di.mesa.metric.common.AlarmConstants;
import com.di.mesa.metric.model.MAlarmCondition;
import com.di.mesa.metric.model.MAlarm;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


//TODO 可以使用策略模式重构
public class AlarmJudgement {

    private DataAlarmTool dataAlarmTool;
    private MAlarm malarm;
    private Map<String, Double> monitoredMapValue;

    public AlarmJudgement(DataAlarmTool dataAlarmTool, MAlarm malarm, Map<String, Double> monitoredMapValue) {
        this.malarm = malarm;
        this.monitoredMapValue = monitoredMapValue;
        // this.monitorBuffer = monitorBuffer;
        this.dataAlarmTool = dataAlarmTool;
    }

    // TODO 新的类型，在取数据的时候，存在问题，tree类型的有问题 下午看下
    public boolean shouldAlarm() throws IOException {
        return judgeAlarm(this.malarm, this.monitoredMapValue);
    }

    private boolean judgeAlarm(MAlarm malarm, Map<String, Double> monitoredMapValue) throws IOException {
        boolean isConnOrType = malarm.getConnectionType().equals(AlarmConstants.ALARM_CONN_TYPE_OR) ? true : false;
        boolean isAlarm = isConnOrType ? false : true;
        malarm.setCycleStart(malarm.getStart());
        malarm.setCycleEnd(malarm.getEnd());
        boolean shouldRecord = true;
        isAlarm = judgeAlarmByConditions(malarm, monitoredMapValue, shouldRecord, isConnOrType, isAlarm);

        if (dataAlarmTool.logger.isDebugEnabled()) {
            StringBuffer logBuffer = new StringBuffer();
            logBuffer.append("token jobid ").append(malarm.getmAlarmId()).append(" isConnOrType ").append(isConnOrType)
                    .append(" judge result ").append(isAlarm);
            dataAlarmTool.logger.debug(logBuffer.toString());
        }

        int triggerCount = malarm.getAlarmTriggerCountInInteger();

        if (isAlarm && triggerCount > 1) {
            shouldRecord = false;
            for (int c = 1; c < triggerCount; c++) {

                long cycleStart = malarm.getStart() - malarm.getAlarmFrequency() * c;
                long cycleEnd = malarm.getEnd() - malarm.getAlarmFrequency() * c;
                malarm.setCycleStart(cycleStart);
                malarm.setCycleEnd(cycleEnd);

                Map<String, String> responseMapInfo = dataAlarmTool.getOpentsData(true, malarm, malarm.getMalarmConditions(), cycleStart,
                        cycleEnd, malarm.getComputeDimension());

                String data = responseMapInfo.get("reponseBody");
                if (data == null || data.length() == 0) {
                    isAlarm = false;
                    break;
                }

                Map<String, Double> cycleMapValue = dataAlarmTool.getMonitoredMapValue(malarm, data);

                // 告警文案 需要调整处理
                isAlarm &= judgeAlarmByConditions(malarm, cycleMapValue, shouldRecord, isConnOrType, isAlarm);

                if (!isAlarm) {
                    break;
                }
            }
        }

        return isAlarm;
    }

    private boolean judgeAlarmByConditions(MAlarm malarm, Map<String, Double> monitoredMapValue, boolean shouldRecord,
                                           boolean isConnOrType, boolean isAlarm) throws IOException {

        // 每种类型的告警条件单独拼接成内容
        Set<String> monitoredValueSet = new HashSet<String>();
        for (MAlarmCondition alarmCondition : malarm.getMalarmConditions()) {
            double monitoredValue = -1l;

            String fieldKey = malarm.isMapCompute() ? alarmCondition.getMapComputeKey() : malarm.getComputeDimension()
                    .getComputeField();
            if (monitoredMapValue.containsKey(fieldKey)) {
                monitoredValue = monitoredMapValue.get(fieldKey);
                if (monitoredValue > 0) {

                    if (malarm.isMapCompute()) {
                        monitoredValueSet.add(fieldKey + ":" + dataAlarmTool.getDouble(monitoredValue));
                    } else {
                        monitoredValueSet.add(dataAlarmTool.getDouble(monitoredValue).toString());
                    }
                }
            }

            if (dataAlarmTool.logger.isDebugEnabled()) {
                StringBuffer logBuffer = new StringBuffer();
                logBuffer.append("token jobid ").append(malarm.getmAlarmId()).append(" on group ")
                        .append(malarm.getAlarmMetric()).append(" ,condition type ")
                        .append(AlarmConstants.getAlarmType(alarmCondition.getType())).append(" threshold ")
                        .append(alarmCondition.getThreshold()).append(",monitoredValue ").append(fieldKey).append(" = ")
                        .append(monitoredValue);
                dataAlarmTool.logger.debug(logBuffer.toString());
            }

            if (isConnOrType) {
                isAlarm |= conditionJudge(malarm, shouldRecord, alarmCondition, monitoredValue);
            } else {
                isAlarm &= conditionJudge(malarm, shouldRecord, alarmCondition, monitoredValue);
                // 只要一个不满足就不用计算接下来的
                if (!isAlarm) {
                    dataAlarmTool.logger.info("token jobid {} on jobgroup {} , condition " + AlarmConstants.getAlarmType(alarmCondition.getType()) + " not fullfil [one of and] ," +
                            "would break", malarm.getmAlarmId(), malarm.getAlarmMetric());
                    break;
                }
            }
        }
        if (isAlarm) {
            malarm.setMonitoredValue(monitoredValueSet.toString().substring(1,
                    monitoredValueSet.toString().length() - 1));
        }
        return isAlarm;
    }

    private boolean conditionJudge(MAlarm malarm, boolean shouldRecord, MAlarmCondition alarmCondition,
                                   double monitoredValue) throws IOException {
        boolean isAlarm = false;
        switch (alarmCondition.getType()) {
            case AlarmConstants.ALARM_TYPE_COMPARED_RATION:
                isAlarm = judgeCycleResult(shouldRecord, monitoredValue, alarmCondition, malarm);
                break;
            case AlarmConstants.ALARM_TYPE_SUROUND_RATION:
                isAlarm = judgeCycleResult(shouldRecord, monitoredValue, alarmCondition, malarm);
                break;
            case AlarmConstants.ALARM_TYPE_MAX_VALUE:
                isAlarm = judgeMaxOrMinResult(shouldRecord, alarmCondition, malarm, monitoredValue);
                break;
            case AlarmConstants.ALARM_TYPE_MIN_VALUE:
                isAlarm = judgeMaxOrMinResult(shouldRecord, alarmCondition, malarm, monitoredValue);
                break;
            case AlarmConstants.ALARM_TYPE_CONTINOUS_COMPARED_RATION:
                isAlarm = judgeContinuousComparedRationResult(shouldRecord, alarmCondition, malarm, monitoredValue);
                break;
            default:
                isAlarm = judgeValueResult(shouldRecord, monitoredValue, alarmCondition, malarm);
                break;
        }
        return isAlarm;
    }

    private boolean judgeContinuousComparedRationResult(boolean shouldRecord, MAlarmCondition alarmCondition,
                                                        MAlarm malarm, double monitoredValue) throws IOException {

        int count = alarmCondition.getPeriod() / 60; //数据库中默认是输入的60
        alarmCondition.setPeriodBak(alarmCondition.getPeriod());
        boolean fullfil = true;

        for (int i = 1; i <= count; i++) {
            alarmCondition.setPeriod(60 * i);
            double cycleValue = getCycledValue(alarmCondition, malarm);

            if (cycleValue < 0) {
                dataAlarmTool.logger.debug(malarm.getAlarmItemName() + " threshold "
                        + alarmCondition.getThresholdDoubleType() + " , cycleValue " + cycleValue);
                return false;
            }

            fullfil &= cycleJudge(shouldRecord, monitoredValue, alarmCondition, malarm, cycleValue);
            if (dataAlarmTool.logger.isDebugEnabled()) {
                StringBuffer logBuffer = new StringBuffer();
                logBuffer.append("token jobid ").append(malarm.getmAlarmId()).append(" type ")
                        .append(AlarmConstants.getAlarmType(alarmCondition.getType())).append(" count ").append(i).append(" threshold ")
                        .append(alarmCondition.getThreshold()).append(" compute value ").append(cycleValue)
                        .append(" result ").append(fullfil);
            }

            if (!fullfil) {
                break;
            }
        }
        return fullfil;
    }

    private boolean judgeValueResult(boolean shouldRecord, double monitorValue, MAlarmCondition alarmCondition,
                                     MAlarm malarm) throws IOException {

        boolean fullfil = valueJudge(shouldRecord, monitorValue, alarmCondition, malarm);

        if (dataAlarmTool.logger.isDebugEnabled()) {
            StringBuffer logBuffer = new StringBuffer();
            logBuffer.append("token jobid ").append(malarm.getmAlarmId())
                    .append(AlarmConstants.getAlarmType(alarmCondition.getType())).append(" threshold ")
                    .append(alarmCondition.getThreshold()).append(" compute value ").append(monitorValue)
                    .append(" result ").append(fullfil);
        }

        return fullfil;
    }

    private boolean valueJudge(boolean shouldRecord, double monitorValue, MAlarmCondition alarmCondition, MAlarm malarm) {

        if (alarmCondition.getType() == AlarmConstants.ALARM_TYPE_OLDER) {
            if (monitorValue > alarmCondition.getThresholdDoubleType()) {
                if (shouldRecord)
                    malarm.getCommonJudgeBuffer().append(",")
                            .append(malarm.isMapCompute() ? alarmCondition.getMapComputeKey() : "").append(">")
                            .append(alarmCondition.getThresholdDoubleType());
                return true;
            }
        } else if (alarmCondition.getType() == AlarmConstants.ALARM_TYPE_OLDER_EQUAL) {
            if (monitorValue >= alarmCondition.getThresholdDoubleType()) {
                if (shouldRecord)
                    malarm.getCommonJudgeBuffer().append(",")
                            .append(malarm.isMapCompute() ? alarmCondition.getMapComputeKey() : "").append(">=")
                            .append(alarmCondition.getThresholdDoubleType());
                return true;
            }
        } else if (alarmCondition.getType() == AlarmConstants.ALARM_TYPE_LOWER) {
            if (monitorValue < alarmCondition.getThresholdDoubleType()) {
                if (shouldRecord)
                    malarm.getCommonJudgeBuffer().append(",")
                            .append(malarm.isMapCompute() ? alarmCondition.getMapComputeKey() : "").append("<")
                            .append(alarmCondition.getThresholdDoubleType());
                return true;
            }
        } else if (alarmCondition.getType() == AlarmConstants.ALARM_TYPE_LOWER_EQUAL) {
            if (monitorValue <= alarmCondition.getThresholdDoubleType()) {
                if (shouldRecord)
                    malarm.getCommonJudgeBuffer().append(",")
                            .append(malarm.isMapCompute() ? alarmCondition.getMapComputeKey() : "").append("<=")
                            .append(alarmCondition.getThresholdDoubleType());
                return true;
            }
        }

        return false;
    }

    private boolean judgeMaxOrMinResult(boolean shouldRecord, MAlarmCondition alarmCondition, MAlarm malarm,
                                        double monitoredValue) throws IOException {

        boolean fullfil = maxOrMinJudge(alarmCondition, monitoredValue);

        if (dataAlarmTool.logger.isDebugEnabled()) {
            StringBuffer logBuffer = new StringBuffer();
            logBuffer.append("token jobid ").append(malarm.getmAlarmId()).append(" type ")
                    .append(AlarmConstants.getAlarmType(alarmCondition.getType())).append(" threshold ")
                    .append(alarmCondition.getThreshold()).append(" compute value ").append(monitoredValue)
                    .append(" result ").append(fullfil);
        }

        if (fullfil && shouldRecord) {
            malarm.getCommonJudgeBuffer().append(",")
                    .append(malarm.isMapCompute() ? alarmCondition.getMapComputeKey() : "")
                    .append(alarmCondition.getType() == AlarmConstants.ALARM_TYPE_MAX_VALUE ? ">" : "<")
                    .append(alarmCondition.getThreshold());
            return true;
        }

        return false;
    }

    private boolean maxOrMinJudge(MAlarmCondition alarmCondition, double monitoredValue) {
        boolean fullfil = false;
        switch (alarmCondition.getType()) {
            case AlarmConstants.ALARM_TYPE_MAX_VALUE:
                if (monitoredValue > alarmCondition.getThresholdDoubleType()) {
                    fullfil = true;
                }
                break;
            case AlarmConstants.ALARM_TYPE_MIN_VALUE:
                if (monitoredValue < alarmCondition.getThresholdDoubleType()) {
                    fullfil = true;
                }
                break;
            default:
                break;
        }
        return fullfil;
    }

    private boolean judgeCycleResult(boolean shouldRecord, double monitoredValue, MAlarmCondition alarmCondition,
                                     MAlarm malarm) throws IOException {

        // double cycleValue = getCycledValue(alarmCondition, malarm.getStart(),
        // malarm.getEnd(), malarm);
        double cycleValue = getCycledValue(alarmCondition, malarm);

        if (cycleValue < 0) {
            dataAlarmTool.logger.debug(malarm.getAlarmItemName() + " threshold "
                    + alarmCondition.getThresholdDoubleType() + " , cycleValue " + cycleValue);
            return false;
        }

        boolean fullfil = cycleJudge(shouldRecord, monitoredValue, alarmCondition, malarm, cycleValue);

        if (dataAlarmTool.logger.isDebugEnabled()) {
            StringBuffer logBuffer = new StringBuffer();
            logBuffer.append("token jobid ").append(malarm.getmAlarmId()).append(" type ")
                    .append(AlarmConstants.getAlarmType(alarmCondition.getType())).append(" threshold ")
                    .append(alarmCondition.getThreshold()).append(" compute value ").append(cycleValue)
                    .append(" result ").append(fullfil);
        }

        return fullfil;
    }

    private double getCycledValue(MAlarmCondition alarmCondition, MAlarm malarm) throws IOException {
        long cycleStart = malarm.getCycleStart() - alarmCondition.getPeriod();
        long cycleEnd = malarm.getCycleEnd() - alarmCondition.getPeriod();

        Map<String, String> responseMapInfo = dataAlarmTool.getOpentsData(true, malarm, Collections.singletonList(alarmCondition), cycleStart,
                cycleEnd, malarm.getComputeDimension());

        String data = responseMapInfo.get("reponseBody");

        if (data == null || data.length() == 0 || data.equals("[]")) {
            return 0;
        }

        Map<String, Double> cycleMapValue = dataAlarmTool.getMonitoredMapValue(malarm, data);

        double cycleValue = cycleMapValue.get(malarm.isMapCompute() ? alarmCondition.getMapComputeKey() : malarm
                .getComputeDimension().getComputeField());
        return cycleValue;
    }

    private boolean cycleJudge(boolean shouldRecord, double monitoredValue, MAlarmCondition alarmCondition,
                               MAlarm malarm, double cycleValue) {

        switch (alarmCondition.getSlideType()) {
            case AlarmConstants.ALARM_SLIDE_TYPE_UP:
                double cycleUpValue = (monitoredValue - cycleValue) / cycleValue * 100;
                dataAlarmTool.logger.debug("token jobid " + malarm.getmAlarmId() + " ,alarmItemName "
                        + malarm.getAlarmItemName() + " threshold " + alarmCondition.getThresholdDoubleType()
                        + " , monitor " + cycleUpValue);
                if (cycleUpValue > alarmCondition.getThresholdDoubleType() && shouldRecord) {
                    malarm.getComparedJudgeBuffer().append("，");
                    if (malarm.isMapCompute()) {
                        malarm.getComparedJudgeBuffer().append(alarmCondition.getMapComputeKey())
                                .append(" ")
                                .append(dataAlarmTool.getToken(alarmCondition.getType()))
                                .append(dataAlarmTool.getPeriod(alarmCondition.getPeriod())).append("m").append("上升")
                                .append(dataAlarmTool.getDouble(cycleUpValue)).append("%");
                    } else {
                        // malarm.getComparedJudgeBuffer().append(malarm.isMapCompute()
                        // ? alarmCondition.getMapComputeKey() : "")
                        // .append(dataAlarmTool.getToken(alarmCondition.getType()))
                        // .append(dataAlarmTool.getPeriod(alarmCondition.getPeriod())).append("m").append("上升")
                        malarm.getComparedJudgeBuffer().append(dataAlarmTool.getPeriod(alarmCondition.getPeriod()))
                                .append("m ").append("+").append(dataAlarmTool.getDouble(cycleUpValue)).append("%");
                    }

                    return true;
                }
                break;
            case AlarmConstants.ALARM_SLIDE_TYPE_DOWN:
                double cycleDownValue = (cycleValue - monitoredValue) / cycleValue * 100;
                dataAlarmTool.logger.debug("token jobid " + malarm.getmAlarmId() + " ,alarmItemName "
                        + malarm.getAlarmItemName() + " threshold " + alarmCondition.getThresholdDoubleType()
                        + " , monitor " + cycleDownValue);
                if (cycleDownValue > alarmCondition.getThresholdDoubleType() && shouldRecord) {
                    malarm.getComparedJudgeBuffer().append("，");

                    if (malarm.isMapCompute()) {
                        malarm.getComparedJudgeBuffer().append(alarmCondition.getMapComputeKey())
                                .append(" ")
                                .append(dataAlarmTool.getToken(alarmCondition.getType()))
                                .append(dataAlarmTool.getPeriod(alarmCondition.getPeriod())).append("m").append("下降")
                                .append(dataAlarmTool.getDouble(cycleDownValue)).append("%");
                    } else {
                        malarm.getComparedJudgeBuffer().append(dataAlarmTool.getPeriod(alarmCondition.getPeriod()))
                                .append("m ").append("-").append(dataAlarmTool.getDouble(cycleDownValue)).append("%");
                    }
                    return true;
                }
                break;
            default:
                double cycleSlideValue = (monitoredValue - cycleValue) / cycleValue * 100;
                dataAlarmTool.logger.debug("token jobid " + malarm.getmAlarmId() + " ,alarmItemName "
                        + malarm.getAlarmItemName() + " threshold " + alarmCondition.getThresholdDoubleType()
                        + " , monitor " + cycleSlideValue);
                if (Math.abs(cycleSlideValue) > alarmCondition.getThresholdDoubleType() && shouldRecord) {
                    malarm.getComparedJudgeBuffer().append("，");
                    if (malarm.isMapCompute()) {
                        malarm.getComparedJudgeBuffer().append(alarmCondition.getMapComputeKey())
                                .append(" ")
                                .append(dataAlarmTool.getToken(alarmCondition.getType()))
                                .append(dataAlarmTool.getPeriod(alarmCondition.getPeriod())).append("m")
                                .append(cycleSlideValue > 0 ? "上升" : "下降")
                                .append(dataAlarmTool.getDouble(Math.abs(cycleSlideValue))).append("%");
                    } else {
                        malarm.getComparedJudgeBuffer().append(dataAlarmTool.getPeriod(alarmCondition.getPeriod()))
                                .append("m ").append(cycleSlideValue > 0 ? "+" : "-")
                                .append(dataAlarmTool.getDouble(Math.abs(cycleSlideValue))).append("%");
                    }

                    return true;
                }
                break;
        }

        return false;
    }
}

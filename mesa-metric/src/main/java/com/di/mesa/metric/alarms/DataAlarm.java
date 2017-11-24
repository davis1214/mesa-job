package com.di.mesa.metric.alarms;

import com.di.mesa.metric.common.AlarmConstants;
import com.di.mesa.metric.util.GsonUtil;
import com.di.mesa.metric.util.PropertyUtil;
import com.di.mesa.metric.client.ShuffledOpentsdbClient;
import com.di.mesa.metric.model.MAlarm;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

/**
 * 数据监控告警检查
 *
 * @param
 * @return
 * @throws IOException
 */
public class DataAlarm extends Alarm {

    private ShuffledOpentsdbClient opentsdbDao;
    private DataAlarmTool dataAlarmTool;

    public DataAlarm(String opentsdbUrls) {
        super();
        this.opentsdbDao = new ShuffledOpentsdbClient(opentsdbUrls);
        dataAlarmTool = new DataAlarmTool(opentsdbDao, simpleDateFormat, logger);
    }

    @Override
    public int execute(MAlarm malarm) throws Exception {
        //怎样查询数据  ,主要是如何拼tags

        Map<String, String> reponseMapInfo = dataAlarmTool.getOpentsData(false, malarm, malarm.getMalarmConditions(), malarm.getStart(),
                malarm.getEnd(), malarm.getComputeDimension());

        String data = reponseMapInfo.get("reponseBody");
        if (data == null || data.length() == 0) {
            logger.info("token jobid " + malarm.getmAlarmId() + " ,com.di.mesa.metric.alarm item " + malarm.getAlarmItemName()
                    + " ,no data found between " + simpleDateFormat.format(new Date(malarm.getStart() * 1000))
                    + " and " + simpleDateFormat.format(new Date(malarm.getEnd() * 1000)));
            return AlarmConstants.NO_DATA_FOUND;
        }


        int responseStatus = Integer.valueOf(reponseMapInfo.get("status"));
        if (responseStatus > 300) {
            logger.info("token jobid " + malarm.getmAlarmId()
                    + " , http get error at " + simpleDateFormat.format(new Date(malarm.getStart() * 1000))
                    + " and " + simpleDateFormat.format(new Date(malarm.getEnd() * 1000)) + " , error ");

            if (responseStatus == 400) {
                malarm.setIsAlarm(0);

                Map<String, ?> errorInfoMap = GsonUtil.toMap(data);
                Map<String, ?> errorMap = (Map<String, ?>) errorInfoMap.get("error");
                malarm.setErrorCode(responseStatus);
                malarm.setErrorMsg(errorMap.get("details").toString());
            }

            return AlarmConstants.NO_DATA_FOUND;
        }

        //TODO 增加配置错误类型
        Map<String, Double> monitoredMapValue = dataAlarmTool.getMonitoredMapValue(malarm, data);

        // StringBuffer monitorBuffer = new StringBuffer();
        if (monitoredMapValue.size() == 0) {// 如果为 ［］才会有这种情况
            // monitorBuffer.append(", no data found");
            // com.di.mesa.metric.alarm(malarm, monitorBuffer);
            // logger.info(monitorBuffer.toString());
            logger.info("token jobid {} , {} no data found", malarm.getmAlarmId(), malarm.getAlarmMetric()
                    + " ,itemname  " + malarm.getAlarmItemName());
            return AlarmConstants.NO_DATA_FOUND;
        } else {
            AlarmJudgement judement = new AlarmJudgement(dataAlarmTool, malarm, monitoredMapValue);
            if (judement.shouldAlarm()) {
                alarm(malarm);
            }
        }

        return AlarmConstants.DATA_FOUND;
    }


    // 【微店】${index} | 指标项:${item} | ${time} | ${threshold} , 新版本
    protected void alarm(MAlarm malarm) throws Exception {
        StringBuffer alarmMsgBuffer = new StringBuffer();
        String alarmRecord = malarm.getAlarmRecrod();

        long time = (malarm.getStart() * 1000 + malarm.getEnd() * 1000) / 2;
        alarmMsgBuffer.append(malarm
                .getTemplate()
                .replace("${time}", simpleTimeFormat.format(new Date(time)))
                .replace("${value}", String.valueOf(malarm.getMonitoredValue()))
                .replace("${record}", alarmRecord)
                .replace("${name}", malarm.getmAlarmId()));
        // .replace("${conditions}", "{" + malarm.getConditions() + "}"));

        logger.info("token jobid {} ,alarm context {}", malarm.getmAlarmId(), alarmMsgBuffer.toString());

        String enabledInfo = PropertyUtil.getValueByKey("alarm.properties", "alarm.phone.msg.enable");
        boolean enabled = Boolean.valueOf(enabledInfo);

        if (!enabled) {
            return;
        }

        String evnType = PropertyUtil.getValueByKey("alarm.properties", "alarm.evn.type");
        if (AlarmConstants.ENV_TYPE_TEST.equals(evnType)) {
            alarmMsgBuffer.append("【测试】");
        }

        handleAlarm(malarm, alarmMsgBuffer.toString());
    }
}

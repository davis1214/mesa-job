package com.di.mesa.metric.alarms;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.di.mesa.metric.common.AlarmConstants;
import com.di.mesa.metric.job.MsgJob;
import com.di.mesa.metric.model.*;
import com.di.mesa.metric.util.StrUtil;

import com.di.mesa.metric.client.request.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;

public abstract class Alarm {
    public final Logger logger = LoggerFactory.getLogger(Alarm.class);

    protected SimpleDateFormat simpleDateFormat;
    protected SimpleDateFormat simpleTimeFormat;
    protected List<MsgJob> alarmReceiverList;

    public Alarm() {
        //simpleDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm");
        simpleDateFormat = new SimpleDateFormat("HH:mm");
        simpleTimeFormat = new SimpleDateFormat("HH:mm");
        this.alarmReceiverList = new ArrayList<>();
    }

    public abstract int execute(MAlarm malarm) throws Exception;

    public int handleAlarm(MAlarm malarm, String content) throws Exception {
        for (MsgJob msgJob : alarmReceiverList) {
            msgJob.sendMsg(malarm.getAlarmConcerner(), content);
        }

//        //选择是否发送图片
//        String departmentSenders = PropertyUtil.getValueByKey("alarm.properties", "alarm.weixin.pic.department.sender");
//        if (!departmentSenders.contains(malarm.getDepartmentId())) {
//            return 0;
//        }
//
//        String blacklistSenders = PropertyUtil.getValueByKey("alarm.properties", "alarm.weixin.pic.monitor.blacklist.sender");
//        if (blacklistSenders.contains(malarm.getMonitorId() + "")) {
//            return 0;
//        }

        return 0;
    }

    public Alarm addAlarmReceiver(MsgJob msgJob) {
        this.alarmReceiverList.add(msgJob);
        return this;
    }

    protected String getAlarmType(int alarmType) {
        String alarmTypes = "default";

        switch (alarmType) {
            case AlarmConstants.ALARM_TYPE_TOPIC:
                alarmTypes = "alarm_type_topic";
                break;
            case AlarmConstants.ALARM_TYPE_DATA:
                alarmTypes = "alarm_type_data";
                break;
            case AlarmConstants.ALARM_TYPE_STATE:
                alarmTypes = "alarm_type_state";
                break;
            default:
                break;
        }

        return alarmTypes;
    }


    protected long getLastestMonitorTime(String data) {
        if (data == null || data.length() == 0 || data.equals("[]")) {
            return System.currentTimeMillis() / 1000;
        }

        JSONArray array = JSON.parseArray(data);
        AtomicLong lastestTime = new AtomicLong(0l);
        for (int i = 0; i < array.size(); i++) {
            JSONObject object = (JSONObject) array.get(i);
            JSONObject dps = object.getJSONObject("dps");
            for (String key : dps.keySet()) {
                if (Long.valueOf(key) > lastestTime.get()) {
                    lastestTime.set(Long.valueOf(key));
                }
            }
        }
        return lastestTime.get();
    }

    protected List<Filter> getFilter(ComputeDimension computeDimension) {
        List<Filter> filters = new ArrayList<>();

        Filter filter = new Filter();
        filter.setTagk(computeDimension.getComputeField());
        if (computeDimension.getComputeType() > AlarmConstants.ALARM_INDEX_MAP_PV) {
            filter.setType("wildcard");
            filter.setFilter("*");
        } else {
            filter.setType("regexp");
            filter.setFilter(computeDimension.getComputeField());
        }

        filter.setGroupBy(Boolean.TRUE);
        filters.add(filter);

        return filters;
    }

    protected String getMAlarmConcernsTemplate(List<MAlarmConcern> alarmConcerns) {
        String template = "";
        // TODO 默认告警项与告警关注人记录一对一
        if (alarmConcerns != null && alarmConcerns.size() > 0) {
            template = alarmConcerns.get(0).getAlarmInfoTemplate();
        }
        if (template != null && !template.isEmpty()) {
            return template;
        }

        return "com.di.mesa.metric.alarm occurs on module ${module} with index ${index},com.di.mesa.metric.alarm value ${threshold}";
    }

    protected AlarmConcerner getMAlarmConcernsMembers(long alarmId, List<MAlarmConcern> alarmConcerns) {
        List<String> tellers = new ArrayList<>();
        List<String> names = new ArrayList<>();

        for (MAlarmConcern mAlarmConcerns : alarmConcerns) {
            List<ConcernMember> tellerMemebers = getAlarmTellers(mAlarmConcerns.getConcernmembers());
            if (tellerMemebers.size() == 0) {
                continue;
            }

            String tel = null;
            String weixin = null;
            for (int c = 0; c < tellerMemebers.size(); c++) {
                ConcernMember concernMember = tellerMemebers.get(c);
                if (mAlarmConcerns.isPhoneAlarm()) {
                    tel = concernMember.getTel();
                    if (!StrUtil.empty(tel)) {
                        tellers.add(tel);
                    }
                }

                if (mAlarmConcerns.isWeixinAlarm()) {
                    weixin = concernMember.getName();
                    if (!StrUtil.empty(weixin)) {
                        names.add(weixin);
                    }
                }
            }
        }
        return new AlarmConcerner(alarmId, tellers, names);
    }

    protected List<ConcernMember> getAlarmTellers(String concernmembers) {
        List<ConcernMember> array = null;
        try {
            array = JSON.parseArray(concernmembers, ConcernMember.class);
        } catch (JSONException e) {
            logger.error(e.getMessage(), e);
        }
        return array;
    }


}

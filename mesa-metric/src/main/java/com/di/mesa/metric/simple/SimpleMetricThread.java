package com.di.mesa.metric.simple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.di.mesa.metric.common.OpenTSDBException;
import com.di.mesa.metric.job.MsgJob;
import com.di.mesa.metric.job.PhoneMsgJob;
import com.di.mesa.metric.model.AlarmConcerner;
import com.di.mesa.metric.util.GsonUtil;
import com.di.mesa.metric.util.TimeSeriesOperator;
import com.google.common.collect.Maps;

public class SimpleMetricThread extends Thread {

    // = "di_mesa_kudu_order";

    private String metric;
    private List<MsgJob> msgJobList;
    private long delayInSeconds;

    public SimpleMetricThread(String metric, long delayInSeconds) {
        super();
        this.metric = metric;
        this.delayInSeconds = delayInSeconds;

        msgJobList = new ArrayList<>();
        msgJobList.add(new PhoneMsgJob());
        //msgJobList.add(new WeixinMsgJob());

        System.out.println(new Date() + " , " + this.metric + " with delayInSeconds " + delayInSeconds + " has started ");
    }

    @Override
    public void run() {
        System.out.println(new Date() + " , " + metric + " , start to exec " + metric);
        CheckPutCountMetric(this.metric);
        System.out.println(new Date() + " , " + metric + " ,  had executed " + metric);

    }

    private void CheckPutCountMetric(String metric) {
        String urlString = "http://10.1.8.20:4242";
        long endEpoch = System.currentTimeMillis() / 1000;
        //long startEpoch = endEpoch - 5 * 60;
        long startEpoch = endEpoch - this.delayInSeconds;
        Map<String, String> tags = Maps.newHashMap();
        tags.put("bolt.name", "MesaOrderKuduBolt");
        tags.put("PutCount", "PutCount");

        try {
            String json = TimeSeriesOperator.retrieveTimeSeriesWithPostMethod(urlString, startEpoch, endEpoch, metric,
                    tags);

            System.out.println(new Date() + " , " + metric + " , start epoch " + new Date(startEpoch * 1000)
                    + " ,end epoch " + new Date(endEpoch * 1000));
            System.out.println(new Date() + " , " + metric + " with queried json: " + json);

            List<Map<String, ?>> metricInfoList = GsonUtil.toList(json);

            for (Map<String, ?> metricInfoMap : metricInfoList) {
                if (metricInfoMap.containsKey("dps")) {

                    Map<String, ?> dpsMap = (Map<String, ?>) metricInfoMap.get("dps");

                    if (dpsMap.size() > 0) {
                        System.err.println(new Date() + " , " + metric + " , data :" + dpsMap);
                    } else {
                        handleAlarm();
                        System.out.println(new Date() + " , " + metric + " , sorry ,no results!");
                    }
                }
            }

        } catch (OpenTSDBException e) {
            e.printStackTrace();
        }

        System.out.println("end!!");
    }

    private void handleAlarm() {

        for (int i = 0; i < msgJobList.size(); i++) {

            List<String> tellers = new ArrayList<>();
            tellers.add("13691016953,13501016924");
            List<String> weixin = new ArrayList<>();
            tellers.add("hejian,dizhiyong");

            AlarmConcerner alarmConcerners = new AlarmConcerner(0l, tellers, weixin);

            String content = "[告警] kudu数据写入Error，metric " + this.metric;
            try {
                System.out.println(new Date() + " , " + metric + " , has error , alarm content " + content);
                msgJobList.get(i).sendMsg(alarmConcerners, content);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        System.out.println(new Date() + " , " + metric + " , handled alarm ");

    }

}

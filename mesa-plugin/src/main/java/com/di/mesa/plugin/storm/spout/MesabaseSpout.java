package com.di.mesa.plugin.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import com.di.mesa.plugin.opentsdb.ShuffledOpentsdbClient;
import com.di.mesa.plugin.opentsdb.builder.Metric;
import com.di.mesa.plugin.opentsdb.builder.MetricBuilder;
import com.di.mesa.plugin.storm.CommonConfiure;
import com.di.mesa.plugin.storm.bolt.MesaBoltConfiure;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Davi on 17/8/15.
 */
public class MesaBaseSpout extends BaseRichSpout {

    private static final Logger logger = LoggerFactory.getLogger(MesaBaseSpout.class);

    private SpoutOutputCollector collector;


    //signed in or not
    protected AtomicBoolean hasSubscribed;
    protected AtomicBoolean isReady;

    protected String mesaStreamingId = "mesa.spout.streaming.id";

    protected int intervalSecond = 1;

    //metric
    protected final String EmitCount = "EmitCount";
    protected final String EmitCost = "EmitCost";
    protected final String ExecuteCost = "ExecuteCost";


    protected String taskId;
    protected String metricName;

    protected boolean shouldRecordToOpentsdb;
    protected ShuffledOpentsdbClient opentsdbClient;

    //record
    protected List<String> rowLogList = null;
    protected ConcurrentHashMap<String, AtomicLong> MeticInfo = null;

    //metric
    protected AtomicLong costTime = new AtomicLong(0l);
    protected AtomicLong lastTime = new AtomicLong(0l);
    protected AtomicLong lastPrintTime = new AtomicLong(0l);


    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        //this.collector = collector;
        this.taskId = context.getThisComponentId();

        hasSubscribed = new AtomicBoolean(false);
        isReady = new AtomicBoolean(false);

        if (conf.containsKey(MesaSpoutConfigure.MESA_STREAMING_ID)) {
            this.mesaStreamingId = conf.get(MesaSpoutConfigure.MESA_STREAMING_ID).toString();
        }

        MeticInfo = new ConcurrentHashMap<>();
        lastPrintTime.set(System.currentTimeMillis());
        lastTime.set(System.currentTimeMillis());
        costTime.set(System.currentTimeMillis());
        metricName = conf.get(CommonConfiure.MESA_TOPOLOGY_NAME).toString();

        shouldRecordToOpentsdb = false;
        if (conf.containsKey(MesaBoltConfiure.SHOULD_RECORD_METRIC_TO_OPENTSDB)) {
            shouldRecordToOpentsdb = Boolean.valueOf(conf.get(MesaBoltConfiure.SHOULD_RECORD_METRIC_TO_OPENTSDB).toString());

            String opentsdbUrl = conf.get(MesaBoltConfiure.OPENTSDB_URL).toString();
            opentsdbClient = new ShuffledOpentsdbClient(opentsdbUrl);
        }
    }



    public void nextTuple() {
        //super.nextTuple();
    }

    protected void recordMonitorLog() {
        long timeSpan = System.currentTimeMillis() - lastPrintTime.get();
        if (timeSpan > 60 * 1000) {
            logger.info("taskIndex {} , Metic_Info {} ,Time_Span {}", this.taskId, MeticInfo.toString(), timeSpan);

            if (shouldRecordToOpentsdb) {
                recordToOpentsdb();
            }

            MeticInfo.clear();
            lastPrintTime.set(System.currentTimeMillis());
        }
    }

    private void recordToOpentsdb() {
        MetricBuilder builder = MetricBuilder.getInstance();

        long timestamp = System.currentTimeMillis() / 1000;
        Enumeration<String> keys = MeticInfo.keys();
        while (keys.hasMoreElements()) {
            String key = keys.nextElement();
            Long value = MeticInfo.get(key).get();
            Map<String, String> tags = getBasicMetricTags();
            tags.put(key, key); //将监控指标都写入opentsdb
            builder.addMetric(new Metric(metricName, timestamp, value, tags));
        }

        opentsdbClient.putData(builder);
    }

    protected Map<String, String> getBasicMetricTags() {
        Map<String, String> tags = Maps.newHashMap();
        tags.put("spout.name", this.getClass().getSimpleName());
        tags.put("task.index", String.valueOf(this.taskId));
        return tags;
    }

    protected void recordMetric(ConcurrentHashMap<String, AtomicLong> monitorCounter, String metricMonitor) {
        if (!monitorCounter.containsKey(metricMonitor)) {
            monitorCounter.put(metricMonitor, new AtomicLong(0l));
        }
        monitorCounter.get(metricMonitor).getAndIncrement();
    }

    protected void recordMetric(ConcurrentHashMap<String, AtomicLong> monitorCounter, String metricMonitor, long total) {
        if (!monitorCounter.containsKey(metricMonitor)) {
            monitorCounter.put(metricMonitor, new AtomicLong(0l));
        }
        monitorCounter.get(metricMonitor).getAndAdd(total);
    }

    protected void afterExecute() {
        recordMetric(MeticInfo, ExecuteCost, (System.currentTimeMillis() - costTime.get()));
        recordMonitorLog();
    }

    protected void beforeExecute() {
        costTime.set(System.currentTimeMillis());
        recordMetric(MeticInfo, EmitCount);
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //super.declareOutputFields(declarer);
    }


}
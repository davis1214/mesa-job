package com.di.mesa.job.jstorm.blot;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Administrator on 17/7/17.
 */
public class MesaBaseBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(MesaBaseBolt.class);

    protected ConcurrentHashMap<String, AtomicLong> meticCounter = null;

    protected static final String runningMode = "topology.running.type";
    protected static final String runningMode_Local = "local";
    protected static final String runningMode_Cluster = "cluster";


    //TODO 统一定义&约束规范
    protected final String updateSellerIdCost = "updateSellerIdCost";
    protected final String parserCost = "parserCost";
    protected final String TupleCount = "TupleCount";
    protected final String HbasePut = "HbasePut";
    protected final String ConstructHbasePut = "ConstructHbasePut";
    protected final String TableFlushCommits = "TableFlushCommit";
    protected final String TableFlushCommitCost = "FlushCommitCost";
    protected final String TableFlushCount = "TableFlushCount";
    protected final String ExecuteCost = "ExecuteCost";
    protected final String PutCount = "PutCount";
    protected final String PutCost = "PutCost";
    protected final String ErrorCount = "ErrorCount";

    protected final String SendCount = "SendCount";
    protected final String SendCost = "SendCost";
    protected final String SendErrCount = "SendErrCount";

    protected final String BlackListCount = "BlackListCount";

    protected final String AggrCount = "AggrCount";

    protected AtomicLong lastTime = new AtomicLong(0l);
    protected AtomicLong lastPrintTime = new AtomicLong(0l);

    protected boolean shouldRecordToOpentsdb;
    protected Map stormConf;

    private LoadingCache<String, String> cache = null;
    private BufferedWriter writer;

    protected boolean isLocalMode;

    protected int taskIndex;

    @Override
    public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.stormConf = stormConf;

        this.taskIndex = topologyContext.getThisTaskIndex();

        meticCounter = new ConcurrentHashMap<>();
        lastPrintTime.set(System.currentTimeMillis());
        lastTime.set(System.currentTimeMillis());

        shouldRecordToOpentsdb = false;
        if (stormConf.containsKey("busi.record.monitor.to.opentsdb")) {
            shouldRecordToOpentsdb = Boolean.valueOf(stormConf.get("busi.record.monitor.to.opentsdb").toString());
        }

        cache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build(new CacheLoader<String, String>() {
                    @Override
                    public String load(String s) {
                        return null;
                    }
                });


        if (isLocalMode()) {

            isLocalMode = true;
            try {
                writer = new BufferedWriter(new FileWriter("/tmp/" + this.getClass().getSimpleName(),true));
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }else{
            isLocalMode = false;
        }
    }

    @Override
    public void execute(Tuple tuple) {
//        this.execute(tuple);
    }

    protected boolean isLocalMode() {
        return stormConf.get(runningMode).equals(runningMode_Local);
    }

    protected void recordToLocalPath(String line) {
        try {
            this.writer.write(this.taskIndex + "-->" + line);
            this.writer.write("\n");
            this.writer.flush();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }


    protected void recordMonitorLog() {
        if ((System.currentTimeMillis() - lastPrintTime.get()) > 60 * 1000) {
            logger.info("montor taskIndex {} , emitMeticCounter {}", this.taskIndex, meticCounter.toString());
            meticCounter.clear();
            lastPrintTime.set(System.currentTimeMillis());
        }
    }

    protected Map<String, String> getBasicMetricTags() {
        Map<String, String> tags = Maps.newHashMap();
        //tags.put("topology.name", stormConf.get("topName").toString());
        tags.put("bolt.name", this.getClass().getSimpleName());
        tags.put("thread", Thread.currentThread().getName());

        return tags;
    }

    protected void recordCounter(ConcurrentHashMap<String, AtomicLong> monitorCounter, String metricMonitor) {
        if (!monitorCounter.containsKey(metricMonitor)) {
            monitorCounter.put(metricMonitor, new AtomicLong(0l));
        }
        monitorCounter.get(metricMonitor).getAndIncrement();
    }

    protected void recordCounter(ConcurrentHashMap<String, AtomicLong> monitorCounter, String metricMonitor, long total) {
        if (!monitorCounter.containsKey(metricMonitor)) {
            monitorCounter.put(metricMonitor, new AtomicLong(0l));
        }
        monitorCounter.get(metricMonitor).getAndAdd(total);
    }


    protected String getStringValue(Object object) {
        String value = null;

        if (object == null) {
            return value;
        }

        if (object instanceof String) {
            value = object.toString();
        } else if (object instanceof Double) {
            BigDecimal d1 = new BigDecimal(object.toString());
            value = String.valueOf(d1.longValue());
        }
        return value;
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        this.declareOutputFields(outputFieldsDeclarer);
    }
}

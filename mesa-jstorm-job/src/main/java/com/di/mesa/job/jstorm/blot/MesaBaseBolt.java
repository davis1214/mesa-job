package com.di.mesa.job.jstorm.blot;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.di.mesa.common.opentsdb.ShuffledOpentsdbClient;
import com.di.mesa.common.opentsdb.builder.Metric;
import com.di.mesa.common.opentsdb.builder.MetricBuilder;
import com.di.mesa.job.jstorm.configure.CommonConfiure;
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
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by davi on 17/7/17.
 */
public class MesaBaseBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(MesaBaseBolt.class);

    protected ConcurrentHashMap<String, AtomicLong> meticCounter = null;

    protected static final String runningMode = "topology.running.type";
    protected static final String runningMode_Local = "local";
    protected static final String runningMode_Cluster = "cluster";

    //private static final long BUFFER_SIZE = 6 * 1024 * 1024;
    protected final String _UNDER_LINE = "_";

    protected final String UpdateSellerIdCost = "UpdateSellerIdCost";
    protected final String ParserCost = "ParserCost";
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
    protected final String VagueCount = "VagueCount";
    protected final String ErrorData = "ErrorData";
    protected final String EmitCount = "EmitCount";
    protected final String TickCost = "TickCost";

    //metric
    protected AtomicLong costTime = new AtomicLong(0l);
    protected AtomicLong lastTime = new AtomicLong(0l);
    protected AtomicLong lastPrintTime = new AtomicLong(0l);

    protected boolean shouldRecordToOpentsdb;
    protected ShuffledOpentsdbClient opentsdbClient;

    protected Map stormConf;
    protected boolean shouldStartCache = false;
    protected LoadingCache<String, String> cache = null;


    //local test
    private BufferedWriter writer;
    protected boolean isLocalMode = false;
    private List<String> rowLogList = null;

    protected int taskIndex;
    protected boolean shouldEnableWhiteList = true;


    @Override
    public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.stormConf = stormConf;
        this.taskIndex = topologyContext.getThisTaskIndex();

        //metric
        meticCounter = new ConcurrentHashMap<>();
        lastPrintTime.set(System.currentTimeMillis());
        lastTime.set(System.currentTimeMillis());
        costTime.set(System.currentTimeMillis());

        shouldRecordToOpentsdb = false;
        if (stormConf.containsKey(CommonConfiure.SHOULD_RECORD_METRIC_TO_OPENTSDB)) {
            shouldRecordToOpentsdb = Boolean.valueOf(stormConf.get(CommonConfiure.SHOULD_RECORD_METRIC_TO_OPENTSDB).toString());

            String opentsdbUrl = stormConf.get(CommonConfiure.OPENTSDB_URL).toString();
            opentsdbClient = new ShuffledOpentsdbClient(opentsdbUrl);
        }

        //open cache
        if (stormConf.containsKey("busi.cache.should.start")) {
            shouldStartCache = Boolean.valueOf(stormConf.get("busi.cache.should.start").toString());
            if (shouldStartCache) {
                cache = CacheBuilder.newBuilder()
                        .maximumSize(10000)
                        .expireAfterWrite(10, TimeUnit.MINUTES)
                        .build(new CacheLoader<String, String>() {
                            @Override
                            public String load(String s) {
                                return null;
                            }
                        });
            }
        }

        try {
            shouldEnableWhiteList = Boolean.valueOf(stormConf.get(CommonConfiure.BUSI_WHITE_LIST_ENABLED).toString());
        } catch (Exception e) {
            shouldEnableWhiteList = true;
        }

        if (isLocalMode()) {
            rowLogList = new LinkedList<>();

            try {
                writer = new BufferedWriter(new FileWriter("/tmp/" + this.getClass().getSimpleName(), true));
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    protected boolean isTickComponent(Tuple input) {
        return input.getSourceComponent().equals(CommonConfiure.TICK_SPOUT_NAME);
    }

    // white list filter
    protected boolean shouldSkip(String sellerId) {
        if (sellerId == null) {
            return true;
        }

        //return shouldEnableWhiteList && !isLocalMode && !Arrays.asList(CommonConfiure.SELLER_TEST).contains(sellerId);
        return shouldEnableWhiteList && !isLocalMode && !CommonConfiure.WhiteNameList.contains(sellerId);
    }


    @Override
    public void execute(Tuple tuple) {
        //super.execute(tuple);
    }


    protected void afterExecute() {
        //失败后,也丢掉此消息
        recordCounter(meticCounter, ExecuteCost, (System.currentTimeMillis() - costTime.get()));
        recordMonitorLog();
    }

    protected void beforeExecute() {
        costTime.set(System.currentTimeMillis());
        recordCounter(meticCounter, TupleCount);
    }

    protected boolean isLocalMode() {
        if (stormConf.containsKey(runningMode)) {
            isLocalMode = stormConf.get(runningMode).equals(runningMode_Local) ? true : false;
        }

        return isLocalMode;
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

    protected void recordMsgTolocal(String rawLog) {
        rowLogList.add(System.currentTimeMillis() + " ----> " + rawLog);
    }


    protected void recordMonitorLog() {

        long timeSpan = System.currentTimeMillis() - lastPrintTime.get();
        if (timeSpan > 60 * 1000) {
            logger.info("taskIndex {} , Metic_Info {} ,Time_Span {}", this.taskIndex, meticCounter.toString(), timeSpan);

            if (shouldRecordToOpentsdb) {
                recordToOpentsdb();
            }

            meticCounter.clear();
            lastPrintTime.set(System.currentTimeMillis());
        }
    }

    private void recordToOpentsdb() {
        MetricBuilder builder = MetricBuilder.getInstance();
        //String name, long timestamp, Object value, Map<String, String> tags
        String metricName = stormConf.get(CommonConfiure.BUSI_TOPOLOGY_NAME).toString();
        long timestamp = System.currentTimeMillis() / 1000;
        Enumeration<String> keys = meticCounter.keys();
        while (keys.hasMoreElements()) {
            String key = keys.nextElement();
            Long value = meticCounter.get(key).get();
            Map<String, String> tags = getBasicMetricTags();
            tags.put(key, key); //将监控指标都写入opentsdb
            Metric newMetric = new Metric(metricName, timestamp, value, tags);
            builder.addMetric(newMetric);
        }

        opentsdbClient.putData(builder);
    }

    protected Map<String, String> getBasicMetricTags() {
        Map<String, String> tags = Maps.newHashMap();
        tags.put("bolt.name", this.getClass().getSimpleName());
        tags.put("task.index", String.valueOf(this.taskIndex));
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

    protected void procTickTuple(Tuple input) throws IOException {
        if (isLocalMode) {
            for (String rowLog : rowLogList) {
                recordToLocalPath(rowLog);
            }
            rowLogList.clear();
        }

        recordCounter(meticCounter, ExecuteCost, (System.currentTimeMillis() - costTime.get()));
        recordMonitorLog();
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

package com.di.mesa.job.jstorm.spout;


import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.tuple.Values;
import com.vdian.vdianmq.client.MqClientImpl;
import com.vdian.vdianmq.client.consumer.ConsumeListener;
import com.vdian.vdianmq.client.consumer.ConsumeRetryPolicy;
import com.vdian.vdianmq.client.consumer.Context;
import com.vdian.vdianmq.client.consumer.Status;
import com.vdian.vdianmq.client.consumer.pull.PullConsumer;
import com.vdian.vdianmq.model.Message;
import com.vdian.vdianmq.model.Qos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Administrator on 17/7/19.
 */
public class ConsumerService implements ConsumeListener {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerService.class);

    private PullConsumer consumer;
    private MqClientImpl mqClient;
    private SpoutOutputCollector _collector;
    private AtomicLong lastTime = new AtomicLong(0L);
    private AtomicLong lastPrintTime = new AtomicLong(0L);
    private ConcurrentHashMap<String, AtomicLong> meticCounter = null;

    private String vdianmqStreamId;
    private final String TupleCount = "TupleCount";
    private final String EmitCost = "EmitCost";

    private ConsumeRetryPolicy policy = new ConsumeRetryPolicy() {
        public int retryCountOnFailure() {
            return 3;
        }

        public long waitingTimeBeforeRetry(int retryCount) {
            return 1000 * retryCount;
        }
    };

    public ConsumerService(String vdianmqStreamId , SpoutOutputCollector collector, String zkAddress, String consumerGroup) {
        this._collector = collector;
        this.vdianmqStreamId = vdianmqStreamId;

        this.meticCounter = new ConcurrentHashMap();

        this.mqClient = new MqClientImpl();
        this.mqClient.setZkAddress(zkAddress);
        this.mqClient.init();

        this.consumer = new PullConsumer();
        this.consumer.setMqClient(this.mqClient);
        this.consumer.setQos(Qos.AT_LEAST_ONCE);
        this.consumer.setConsumerGroup(consumerGroup);
        this.consumer.init();
    }

    public boolean subscribe(String topic) {
        boolean isSubscribed = false;

        try {
            this.consumer.subscribe(topic, this);
            isSubscribed = true;
        } catch (Exception e) {
            logger.error(e.getMessage() + " <<< subscribed failed ", e);
            isSubscribed = false;
        }

        return isSubscribed;
    }

    private void recordMonitorLog() {
        long timeSpan = System.currentTimeMillis() - this.lastPrintTime.get();
        if (timeSpan > 60000L) {
            logger.info("taskIndex {} , Metic_Info {} , Time_Span {}", Thread.currentThread().getName(), this.meticCounter.toString(), timeSpan);
            this.meticCounter.clear();
            this.lastPrintTime.set(System.currentTimeMillis());
        }
    }

    private void recordCounter(ConcurrentHashMap<String, AtomicLong> monitorCounter, String metricMonitor) {
        if (!monitorCounter.containsKey(metricMonitor)) {
            monitorCounter.put(metricMonitor, new AtomicLong(0L));
        }
        ((AtomicLong) monitorCounter.get(metricMonitor)).getAndIncrement();
    }

    private void recordCounter(ConcurrentHashMap<String, AtomicLong> monitorCounter, String metricMonitor, long total) {
        if (!monitorCounter.containsKey(metricMonitor)) {
            monitorCounter.put(metricMonitor, new AtomicLong(0L));
        }
        ((AtomicLong) monitorCounter.get(metricMonitor)).getAndAdd(total);
    }

    public ConsumeRetryPolicy retryPolicy() {
        return this.policy;
    }

    public Status on(Message message, Context context) {
        try {
            lastTime.set(System.currentTimeMillis());
            recordCounter(this.meticCounter, TupleCount);

            String msg = new String(message.getBody());
            this._collector.emit(vdianmqStreamId, new Values(new Object[]{msg}));

            recordCounter(meticCounter, EmitCost, (System.currentTimeMillis() - lastTime.get()));
            recordMonitorLog();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

        return Status.SUCCESS;
    }
}

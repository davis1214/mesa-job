package com.di.mesa.job.jstorm.spout;


import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by Administrator on 17/7/19.
 */
public class VdianMQSpout extends BaseRichSpout {
    private static final Logger logger = LoggerFactory.getLogger(VdianMQSpout.class);
    private SpoutOutputCollector _collector;
    int intervalSecond = 1;

    private String topic;
    private String zkAddress;
    private String consumerGroup;
    private ConsumerService service;
    private final String vdianmqStreamId = "vdianmq";

    private AtomicBoolean hasSubscribed = new AtomicBoolean(false);

    public VdianMQSpout() {
    }

    public VdianMQSpout(int intervalSecond, String topic, String zkAddress, String consumerGroup) {
        this.intervalSecond = intervalSecond;
        this.topic = topic;
        this.zkAddress = zkAddress;
        this.consumerGroup = consumerGroup;
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        int index = context.getThisTaskIndex();
        //consumerGroup = consumerGroup + "_" + String.valueOf(index);
        this._collector = collector;
        this.service = new ConsumerService(vdianmqStreamId, this._collector, this.zkAddress, this.consumerGroup);

        logger.info("open topic {} consumer {} in name {} ,taskId {}", this.topic, index, this.consumerGroup, index);
    }

    public void nextTuple() {
        logger.info("next tuple called start");

        if (!this.hasSubscribed.get()) {
            boolean isSubscribed = this.service.subscribe(this.topic);
            if (isSubscribed) {
                this.hasSubscribed.getAndSet(true);
            }
        } else {

            //TODO 增加判断,消费服务是否出现了问题
            try {
                Thread.sleep(60000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        logger.info("next tuple called  end");
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        declarer.declare(new Fields(new String[]{"vcontent"}));

        declarer.declareStream(vdianmqStreamId, new Fields(new String[]{"vcontent"}));
    }
}

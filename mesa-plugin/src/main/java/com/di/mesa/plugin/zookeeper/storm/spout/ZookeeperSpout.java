package com.di.mesa.plugin.zookeeper.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import com.di.mesa.plugin.zookeeper.ZookeeperConfigure;

import java.util.Map;

/**
 * Created by Davi on 17/8/15.
 */
public class ZookeeperSpout extends BaseRichSpout {

    private String streamingId = "zookeeper.streaming.id.zk";
    private Map<String, String> zkMap;
    private SpoutOutputCollector _collector;

    private int intervalSecond = 1;
    private int sleepTime = 1000 * this.intervalSecond;

    public ZookeeperSpout(Map<String, String> zkMap, int intervalSecond) {
        this.zkMap = zkMap;
        this.intervalSecond = intervalSecond;
    }


    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this._collector = collector;
        this.sleepTime = 1000 * this.intervalSecond;

        if (zkMap.containsKey(ZookeeperConfigure.ZOOKEEPER_STREAMING_ID)) {
            this.streamingId = zkMap.get(ZookeeperConfigure.ZOOKEEPER_STREAMING_ID).toString();
        }
    }


    public void nextTuple() {

        //TODO 链接zk,查看消息是否进行了同步

        // TODO 通过两种方式  ,定时读 ,响应的方式


    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        for (Map.Entry<String, Integer> entry : zkMap.entrySet()) {
//            declarer.declareStream(entry.getKey(), new Fields("zk"));
//        }
        //declarer.declareStream(entry.getKey(), new Fields("zk"));
    }


}
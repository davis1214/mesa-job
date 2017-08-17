package com.di.mesa.job.jstorm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;

/**
 * Created by davi on 17/8/15.
 */
public class TickSpout extends BaseRichSpout {
    private Map<String, Integer> tickMap;
    private SpoutOutputCollector _collector;
    private int intervalSecond = 1;
    private int count = 0;
    private int sleepTime = 1000 * this.intervalSecond;

    public TickSpout(Map<String, Integer> tickMap, int intervalSecond) {
        this.tickMap = tickMap;
        this.intervalSecond = intervalSecond;
    }


    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this._collector = collector;
        this.sleepTime = 1000 * this.intervalSecond;
    }


    public void nextTuple() {
        while (true) {
            count++;
            for (Map.Entry<String, Integer> entry : tickMap.entrySet()) {

                if (count % entry.getValue() == 0) {
                    _collector.emit(entry.getKey(), new Values(entry.getValue()));
                }
            }

            if (count == 99990) count = 0;
            Utils.sleep(sleepTime);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (Map.Entry<String, Integer> entry : tickMap.entrySet()) {
            declarer.declareStream(entry.getKey(), new Fields("tick"));
        }
    }


}
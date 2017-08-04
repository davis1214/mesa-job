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
 * Created by zhaoxican on 16/5/16.
 */
public class TickSpout extends BaseRichSpout {
    private Map<String,Integer> tickMap ;
    private SpoutOutputCollector _collector;
    int intervalSecond = 1;

    public TickSpout(Map<String,Integer> tickMap , int intervalSecond){
        this.tickMap = tickMap;
        this.intervalSecond = intervalSecond;
    }


    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector){
        this._collector = collector;
    }

    public void nextTuple(){
        int count = 0;
        final int sleepTime = 1000 * this.intervalSecond;
        while (true){
            count++;
            for (Map.Entry<String,Integer> entry:tickMap.entrySet()){

                if (count % entry.getValue()==0){
                    _collector.emit(entry.getKey(),new Values(entry.getValue()));
                }
            }

            if (count == 99990)  count = 0;
            Utils.sleep(sleepTime);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (Map.Entry<String, Integer> entry : tickMap.entrySet()) {
            declarer.declareStream(entry.getKey(), new Fields("tick"));
        }
    }


}
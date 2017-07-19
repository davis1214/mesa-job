package com.di.mesa.job.jstorm.spout;


import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * Created by Administrator on 17/6/29.
 */
public class SimpleDataTestSpout extends BaseRichSpout {

    private SpoutOutputCollector _collector;

    String data = null;

    public SimpleDataTestSpout(String data) {
        super();
        this.data = data;
    }

    public SimpleDataTestSpout() {
        super();
    }

    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {

        _collector = collector;

    }



    public void nextTuple() {
        long count = 0;
        long MAX = 6;
        while (count++ < MAX) {
            Random random = new Random();
            int i = random.nextInt(8000);
            String hash = String.format("%04d", i);

            int j = random.nextInt(8000);
            String hash1 = String.format("%04d", j);
            long time = System.currentTimeMillis();
            String a = String.format("%016x", time);
            _collector.emit(new Values(String.valueOf(System.currentTimeMillis()) + " " + hash + a + "0a010f50" + hash1 + " 0.1.1 1 1|1|/final.do|payCashier|192.1.1.15" +
                    ".80|-|0|15|1|200"));
            // }
            Utils.sleep(1);
        }
        //System.out.println("###"+count);
        // Utils.sleep(1000*60*60);

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("row"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }

}


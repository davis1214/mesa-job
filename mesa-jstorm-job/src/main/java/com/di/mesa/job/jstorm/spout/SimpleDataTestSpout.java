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
 * Created by Administrator on 17/7/19.
 */
public class SimpleDataTestSpout extends BaseRichSpout {
    private SpoutOutputCollector _collector;
    String data = null;

    public SimpleDataTestSpout(String data) {
        this.data = data;
    }

    public SimpleDataTestSpout() {
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this._collector = collector;
    }

    public void nextTuple() {
        long count = 0L;
        long MAX = 6L;
        while (count++ < MAX) {
            Random random = new Random();
            int i = random.nextInt(8000);
            String hash = String.format("%04d", new Object[]{Integer.valueOf(i)});

            int j = random.nextInt(8000);
            String hash1 = String.format("%04d", new Object[]{Integer.valueOf(j)});
            long time = System.currentTimeMillis();
            String a = String.format("%016x", new Object[]{Long.valueOf(time)});
            this._collector.emit(new Values(new Object[]{String.valueOf(System.currentTimeMillis()) + " " + hash + a + "0a010f50" + hash1 + " 0.1.1 1 1|1|/final.do|payCashier|10.1.15.80|-|0|15|1|200"}));

            Utils.sleep(1L);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(new String[]{"row"}));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}

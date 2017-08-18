package com.di.mesa.job.jstorm.blot;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.di.mesa.plugin.storm.bolt.MesaBaseBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;

/**
 * Created by davi on 17/8/1.
 * <p/>
 * 用于验证topology 接收到的tuple会写到本地磁盘文件  /tmp/LocalFileSinkBolt 中
 */
public class LocalFileSinkBolt extends MesaBaseBolt {

    private static final Logger logger = LoggerFactory.getLogger(LocalFileSinkBolt.class);

    private OutputCollector collector;

    public LocalFileSinkBolt() {
    }

    public LocalFileSinkBolt(boolean isLocalMode) {
        this.isLocalMode = isLocalMode;
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        costTime.set(System.currentTimeMillis());

        if (isLocalMode) {
            rowLogList = new LinkedList<>();
        }

        super.prepare(stormConf, context, null);
    }

    public void execute(Tuple input) {
        beforeExecute();

        try {
            if (isTickComponent(input)) {
                procTickTuple(input);
                return;
            }

            String rawLog = input.getString(0);
            rowLogList.add(rawLog);
        } catch (Exception e) {
            recordCounter(meticCounter, ErrorCount);
            logger.error(e.getMessage(), e);
        }

        collector.ack(input);
        afterExecute();
    }


    protected void procTickTuple(Tuple input) throws IOException {
        collector.ack(input);
        super.procTickTuple(input);
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("trade_notify", new Fields("field"));
    }

}

package com.di.mesa.job.jstorm.blot;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.di.mesa.job.jstorm.bean.Pair;
import com.di.mesa.job.jstorm.bean.TradeCustomer;
import com.di.mesa.job.jstorm.common.TpsCounter;
import com.di.mesa.job.jstorm.configure.SequenceTopologyConfigure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class SplitRecord implements IBasicBolt {

    public static Logger LOG = LoggerFactory.getLogger(SplitRecord.class);

    private TpsCounter tpsCounter;

    public void prepare(Map conf, TopologyContext context) {

        tpsCounter = new TpsCounter(context.getThisComponentId() +
                ":" + context.getThisTaskId());

        LOG.info("Successfully do prepare");


    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        tpsCounter.count();

        Long tupleId = tuple.getLong(0);
        Object obj = tuple.getValue(1);

        if (obj instanceof TradeCustomer) {

            TradeCustomer tradeCustomer = (TradeCustomer) obj;

            Pair trade = tradeCustomer.getTrade();
            Pair customer = tradeCustomer.getCustomer();

            collector.emit(SequenceTopologyConfigure.TRADE_STREAM_ID,
                    new Values(tupleId, trade));

            collector.emit(SequenceTopologyConfigure.CUSTOMER_STREAM_ID,
                    new Values(tupleId, customer));
        } else if (obj != null) {
            LOG.info("Unknow type " + obj.getClass().getName());
        } else {
            LOG.info("Nullpointer ");
        }

    }


    public void cleanup() {

        tpsCounter.cleanup();
        LOG.info("Finish cleanup");

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(SequenceTopologyConfigure.TRADE_STREAM_ID, new Fields("ID", "TRADE"));
        declarer.declareStream(SequenceTopologyConfigure.CUSTOMER_STREAM_ID, new Fields("ID", "CUSTOMER"));
    }

    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }
}
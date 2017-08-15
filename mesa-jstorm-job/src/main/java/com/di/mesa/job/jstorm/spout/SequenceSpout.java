package com.di.mesa.job.jstorm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.di.mesa.job.jstorm.bean.Pair;
import com.di.mesa.job.jstorm.bean.PairMaker;
import com.di.mesa.job.jstorm.bean.TradeCustomer;
import com.di.mesa.job.jstorm.metric.TpsCounter;
import com.di.mesa.job.jstorm.configure.SequenceTopologyConfigure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Created by davi on 17/8/3.
 */
public class SequenceSpout implements IRichSpout {
    private static final long serialVersionUID = 1L;

    public static final Logger LOG = LoggerFactory.getLogger(SequenceSpout.class);

    SpoutOutputCollector collector;

    // I special use long not AtomicLong to check competition
    private long tupleId;
    private long succeedCount;
    private long failedCount;

    private TpsCounter tpsCounter;

    private boolean isFinished;


    private boolean isLimited = false;

    public boolean isDistributed() {
        return true;
    }

    public void SequenceSpout() {

    }

    public void SequenceSpout(boolean isLimited) {
        this.isLimited = isLimited;
    }

    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        this.collector = collector;
        isFinished = false;
        tpsCounter = new TpsCounter(context.getThisComponentId() + ":" + context.getThisTaskId());
    }

    private AtomicLong tradeSum = new AtomicLong(0);
    private AtomicLong customerSum = new AtomicLong(0);

    public void emit() {

        Pair trade = PairMaker.makeTradeInstance();
        Pair customer = PairMaker.makeCustomerInstance();

        TradeCustomer tradeCustomer = new TradeCustomer();
        tradeCustomer.setTrade(trade);
        tradeCustomer.setCustomer(customer);

        tradeSum.addAndGet(trade.getValue());
        customerSum.addAndGet(customer.getValue());

        collector.emit(new Values(tupleId, tradeCustomer), Long.valueOf(tupleId));

        tupleId++;

        tpsCounter.count();
    }

    public void nextTuple() {
        if (isLimited == false) {
            emit();

            return;
        }

        if (isFinished == true) {
            return;
        }

        if (tupleId > SequenceTopologyConfigure.MAX_MESSAGE_COUNT) {
            isFinished = true;
            return;
        }
    }

    public void close() {

        tpsCounter.cleanup();
        LOG.info("Sending :" + tupleId +
                ", success:" + succeedCount +
                ", failed:" + failedCount);
        LOG.info("tradeSum:" + tradeSum + ",cumsterSum" + customerSum);
    }


    public void ack(Object id) {
        Long tupleId = (Long) id;

        succeedCount++;
        return;
    }

    public void fail(Object id) {

        failedCount++;
        Long failId = (Long) id;
        LOG.info("Failed to handle " + failId);

        return;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ID", "RECORD"));
//		declarer.declare(new Fields("ID"));
    }

    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void activate() {
        // TODO Auto-generated method stub
        LOG.info("Start active");
    }

    @Override
    public void deactivate() {
        // TODO Auto-generated method stub
        LOG.info("Start deactive");

    }

}
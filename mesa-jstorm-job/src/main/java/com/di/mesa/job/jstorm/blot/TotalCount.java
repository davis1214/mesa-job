package com.di.mesa.job.jstorm.blot;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.di.mesa.job.jstorm.bean.TradeCustomer;
import com.di.mesa.job.jstorm.common.TpsCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class TotalCount implements IRichBolt {
    public static Logger LOG         = LoggerFactory.getLogger(TotalCount.class);
    
    private OutputCollector collector;
    private TpsCounter          tpsCounter;
    
    @Override
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        // TODO Auto-generated method stub
        this.collector = collector;
        
        tpsCounter = new TpsCounter(context.getThisComponentId() +
                ":" + context.getThisTaskId());
        
        LOG.info("Finished preparation");
    }
    
    private AtomicLong tradeSum    = new AtomicLong(0);
    private AtomicLong customerSum = new AtomicLong(1);
    
    @Override
    public void execute(Tuple input) {
        
        Long tupleId = input.getLong(0);
        TradeCustomer tradeCustomer = (TradeCustomer) input.getValue(1);
        
        tradeSum.addAndGet(tradeCustomer.getTrade().getValue());
        customerSum.addAndGet(tradeCustomer.getCustomer().getValue());
        
        collector.ack(input);
        
        long now = System.currentTimeMillis();
        long spend = now - tradeCustomer.getTimestamp();
        
        tpsCounter.count(spend);
    }
    
    public void cleanup() {
        tpsCounter.cleanup();
        LOG.info("tradeSum:" + tradeSum + ",cumsterSum" + customerSum);
        LOG.info("Finish cleanup");
    }
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        
    }
    
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }
    
}

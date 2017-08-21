package com.di.mesa.plugin.kudu.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.di.mesa.common.exception.MesaException;
import com.di.mesa.plugin.kudu.KuduConfigure;
import com.di.mesa.plugin.kudu.KuduOperationsProducer;
import com.di.mesa.plugin.storm.bolt.MesaBaseBolt;
import org.apache.commons.lang.StringUtils;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Created by Davi on 17/8/21.
 */
public class KuduBolt extends MesaBaseBolt {

    private static Logger logger = LoggerFactory.getLogger(KuduBolt.class);

    private OutputCollector collector = null;
    private Map stormConf;

    private String tableName;
    private String masterAddress;
    private String kuduStreamingId;
    private long batchSize = 100L;
    private long timeoutMillis = 30000L;
    private boolean ignoreDuplicateRows = true;

    private long intervalSleep = 2000l;


    private String kuduProducerString;
    private KuduClient kuduClient;
    private KuduOperationsProducer kuduProducer;
    private KuduSession session;

    private final String Pending_Cost = "Pending_Cost";
    private final String Pending_Count = "Pending_Count";


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.stormConf = stormConf;
        super.prepare(stormConf, context, collector);

        prepareKuduConf(stormConf);

        try {
            this.kuduProducerString = getStringValue(stormConf.get(KuduConfigure.KUDU_BOLT_PRODUCER));
            Class<? extends KuduOperationsProducer> clazz = (Class<? extends KuduOperationsProducer>) Class.forName(this.kuduProducerString);
            kuduProducer = clazz.newInstance();

            // configure producer of kudu
            kuduClient = new KuduClient.KuduClientBuilder(this.masterAddress).build();
            KuduTable kuduTable = kuduClient.openTable(this.tableName);
            kuduProducer.configure(stormConf);
            kuduProducer.initialize(kuduTable);

            session = kuduClient.newSession();
            session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
            session.setTimeoutMillis(timeoutMillis);
            session.setIgnoreAllDuplicateRows(ignoreDuplicateRows);

        } catch (InstantiationException e) {
            logger.error(e.getMessage(), e);
        } catch (IllegalAccessException e) {
            logger.error(e.getMessage(), e);
        } catch (ClassNotFoundException e) {
            logger.error(e.getMessage(), e);
        } catch (KuduException e) {
            logger.error(e.getMessage(), e);
        }

        logger.info("task id {} started , congratulations ~~~ ", this.taskIndex);
    }

    private void prepareKuduConf(Map stormConf) {
        this.tableName = getStringValue(stormConf.get(KuduConfigure.KUDU_TABLE_NAME));
        this.masterAddress = getStringValue(stormConf.get(KuduConfigure.KUDU_MASTER_ADDRESS));

        if (stormConf.containsKey(KuduConfigure.KUDU_BOLT_STREAMING_ID)) {
            this.kuduStreamingId = getStringValue(stormConf.get(KuduConfigure.KUDU_BOLT_STREAMING_ID));
        }

        if (stormConf.containsKey(KuduConfigure.KUDU_BOLT_BATCH_SIZE)) {
            this.batchSize = getIntegerValue(stormConf.get(KuduConfigure.KUDU_BOLT_BATCH_SIZE));
        }

        if (stormConf.containsKey(KuduConfigure.KUDU_BOLT_TIME_OUT)) {
            this.timeoutMillis = getIntegerValue(stormConf.get(KuduConfigure.KUDU_BOLT_TIME_OUT));
        }

        if (stormConf.containsKey(KuduConfigure.KUDU_BOLT_IGNORE_DUPLICATE_ROWS)) {
            this.ignoreDuplicateRows = getBooleanValue(stormConf.get(KuduConfigure.KUDU_BOLT_IGNORE_DUPLICATE_ROWS));
        }
    }

    @Override
    public void execute(Tuple tuple) {
        super.beforeExecute();

        procPendingOperactions();

        try {
            // get schema from tuple (type of array)
            List<Operation> operations = kuduProducer.getOperations(tuple);
            for (Operation o : operations) {
                session.apply(o);
            }

            List<OperationResponse> responses = session.flush();
            boolean hasError = false;
            if (responses != null) {
                for (OperationResponse response : responses) {
                    if (response.hasRowError()) {
                        hasError |= true;
                        recordCounter(meticCounter, ErrorCount);
                    } else {
                        recordCounter(meticCounter, PutCost);
                    }
                }
            }

            if (hasError) {
                this.collector.fail(tuple);
            } else {
                this.collector.ack(tuple);
            }
        } catch (KuduException e) {
            logger.error(e.getMessage(), e);
        }catch (MesaException e) {
            logger.error(e.getMessage(), e);
        }

        super.afterExecute();
    }

    private void procPendingOperactions() {
        if (session.hasPendingOperations()) {
            lastTime.set(System.currentTimeMillis());
            while (session.hasPendingOperations()) {
                logger.info("task id {} has pending operations ,would sleep {} ms!", this.taskIndex, this.intervalSleep);

                try {
                    Thread.sleep(intervalSleep);
                } catch (InterruptedException e) {
                }
            }
            recordCounter(meticCounter, Pending_Cost, (System.currentTimeMillis() - lastTime.get()));
        }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (StringUtils.isEmpty(this.kuduStreamingId)) {
            declarer.declare(new Fields(KuduConfigure.KUDU_BOLT_DECLARED_FIELDS));
        } else {
            declarer.declareStream(this.kuduStreamingId, new Fields(KuduConfigure.KUDU_BOLT_DECLARED_FIELDS));
        }
    }


    @Override
    public void cleanup() {
        super.cleanup();
        try {
            if (kuduProducer != null) {
                kuduProducer.close();
            }
            if (kuduClient != null) {
                kuduClient.shutdown();
            }
        } catch (KuduException e) {
            logger.error(e.getMessage(), e);
        }
    }
}

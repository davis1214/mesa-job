package com.di.mesa.plugin.zookeeper.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.di.mesa.plugin.storm.spout.MesaBaseSpout;
import com.di.mesa.plugin.zookeeper.*;
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by Davi on 17/8/15.
 */
public class ZookeeperSpout extends MesaBaseSpout {

    private static final Logger logger = LoggerFactory.getLogger(ZookeeperSpout.class);

    private SpoutOutputCollector collector;

    private Map<String, String> stormConfMap;


    private String streamingId ;
    private ZkClient zkClient;
    private ZkPropertiesHelperFactory helperFactory;

    private String zkServers = "localhost:2181";
    private int connTimeout = 10 * 1000;

    private String zkpath = "/zkSample/conf/test.properties";
    private String zkConfFile = "mesa.properties"; //default zk conf file
    private String zkRootNodePath = "/mesa/conf"; //default zk conf file

    private int intervalSecond = 1;
    private int sleepTime = 1000 * this.intervalSecond;

    public ZookeeperSpout(String streamingId) {
        this.streamingId = streamingId;
    }

    public ZookeeperSpout(Map<String, String> zkMap, int intervalSecond) {
        this.stormConfMap = zkMap;
        this.intervalSecond = intervalSecond;
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.stormConfMap = conf;
        this.collector = collector;
        super.open(conf, context, null);

        this.sleepTime = 1000 * this.intervalSecond;
        openZookeeper(conf);

        logger.info("task id {} has opened", taskId);
    }

    private void openZookeeper(Map conf) {
        if (conf.containsKey(ZookeeperConfigure.ZOOKEEPER_CONF_FILE)) {
            this.zkConfFile = conf.get(ZookeeperConfigure.ZOOKEEPER_CONF_FILE).toString();
        }

        if (conf.containsKey(ZookeeperConfigure.ZOOKEEPER_ROOT_NODE_PATH)) {
            this.zkRootNodePath = conf.get(ZookeeperConfigure.ZOOKEEPER_ROOT_NODE_PATH).toString();
        }

        if (conf.containsKey(ZookeeperConfigure.ZOOKEEPER_MESA_SERVERS)) {
            this.zkServers = conf.get(ZookeeperConfigure.ZOOKEEPER_MESA_SERVERS).toString();
        }

        if (conf.containsKey(ZookeeperConfigure.ZOOKEEPER_MESA_CONN_TIMEOUT)) {
            this.connTimeout = Integer.valueOf(conf.get(ZookeeperConfigure.ZOOKEEPER_MESA_CONN_TIMEOUT).toString());
        }

        this.zkClient = new ZkClient(this.zkServers, this.connTimeout);
        zkClient.setZkSerializer(new ZkStringSerializer("UTF-8"));
        IZkConfigChangeSubscriber configChangeSubscriber = new ZkConfigChangeSubscriber(this.zkClient, this.zkRootNodePath);
        this.helperFactory = new ZkPropertiesHelperFactory(configChangeSubscriber);
    }


    public void nextTuple() {
        if (!hasSubscribed.get()) {
            if (subscribe()) {
                hasSubscribed.set(true);
                logger.info("subscribe {} successed", this.zkConfFile);
            } else {
                logger.info("subscribe {} failed", this.zkConfFile);
            }
        } else {
            logger.info("next tuple called ,but zookeeper has been listened ,would sleep 30 * 1000l ms!");
            try {
                Thread.sleep(30 * 1000l);
            } catch (InterruptedException e) {
            }
        }
    }

    private boolean subscribe() {
        boolean hasSubscribed = false;
        try {
            ZkPropertiesHelper helper = this.helperFactory.getHelper(this.zkConfFile);
            helper.registerListener("listener",
                    new ZkPropertiesHelper.PropertyChangeListener() {
                        public void propertyChanged(String oldValue, String newValue) {
                            //System.out.println("property chaged: oldValue=" + oldValue + ", newValue=" + newValue);
                            lastTime.set(System.currentTimeMillis());
                            recordMetric(MeticInfo, EmitCount);
                            collector.emit(mesaStreamingId, new Values(new Object[]{newValue, oldValue}));
                            recordMetric(MeticInfo, EmitCost, (System.currentTimeMillis() - lastTime.get()));
                            recordMonitorLog();
                        }
                    });

            hasSubscribed = true;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            hasSubscribed = false;
        }
        return hasSubscribed;
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(this.streamingId, new Fields(ZookeeperConfigure.ZOOKEEPER_FIELD_NEW_VALUE, ZookeeperConfigure.ZOOKEEPER_FIELD_OLD_VALUE));
    }

}
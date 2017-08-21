package com.di.mesa.plugin.storm;

/**
 * Created by Davi on 17/8/17.
 */
public class CommonConfiure {

    //key
    public static final String MESA_TOPOLOGY_ENABLE_NOTIFYER = "mesa.topology.enable.zk.notifier";
    public static final String MESA_TOPOLOGY_NAME = "mesa.topology.name";
    public static final String MESA_TOPOLOGY_RUNNING_MODE = "mesa.topology.running.type";

    public static final String SHOULD_RECORD_METRIC_TO_OPENTSDB = "busi.record.monitor.to.opentsdb";
    public static final String OPENTSDB_URL = "busi.record.monitor.opentsdb.url";

    public static final String SPOUT_NAME = "spout.name";
    public static final String BOLT_NAME = "bolt.name";
    public static final String TASK_ID = "task.index";

    public static final String TICK_SPOUT_NAME = "TICK_SPOUT";


    //key & value
    public static final String RUNNING_MODE_LOCAL = "local";
    public static final String RUNNING_MODE_CLUSTER = "cluster";




}

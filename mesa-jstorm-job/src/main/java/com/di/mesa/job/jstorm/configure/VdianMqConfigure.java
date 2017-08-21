package com.di.mesa.job.jstorm.configure;

/**
 * Created by Davi on 17/8/21.
 */
public class VdianMqConfigure {

    //key
    public static final String VDIAN_MQ_SUBSCRIBED_TOPIC_NAME = "mesa.vdian.mq.subscribe.topic.name";
    public static final String VDIAN_MQ_SUBSCRIBED_TOPIC_TAGS = "mesa.vdian.mq.subscribe.topic.tags";
    public static final String VDIAN_MQ_SUBSCRIBED_TOPIC_GROUPNAME = "mesa.vdian.mq.subscribe.topic.group.name";
    public static final String VDIAN_MQ_SUBSCRIBED_ZK_ADDRESS = "mesa.vdian.mq.subscribe.zk.address";


    //value
    public static final String VDIANMQ = "vdianmq";

    public static String VMQ_DAILY_ADDRESS = "zk1.daily.demo.com,zk2.daily.demo.com,zk3.daily.demo.com";
    public static String VMQ_ONLINE_ADDRESS = "zk1.demo.com,zk2.demo.com";

    public final static String MQ_DEFAULT_TAG = "hbaseData";
    public final static String MQ_TOPIC_ORDER_ALL_STATUS = "order_all_status";


}

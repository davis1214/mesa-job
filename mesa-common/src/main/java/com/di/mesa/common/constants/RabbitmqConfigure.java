package com.di.mesa.common.constants;

/**
 * Created by davi on 17/8/1.
 */
public class RabbitmqConfigure {

    public final static String RABBITMQ_DEFAULT_STREAM_ID = "rabbitmq";

    //是否是持久化的queue
    public final static boolean DURABLE = true;
    //受当前连接限制的queue，true：当与消费者（consumer）断开连接的时候，这个队列应当被立即删除。
    public final static boolean EXCLUSIVE = false;
    //是否自动删除，如果长时间没有用到自动删除
    public final static boolean AUTOD_ELETE = false;
    //自动确认消息,   false 不自动确认，要手动确认消息
    public final static boolean AUTOACK = false;

    public final static String VHOST_MARKER = "busi.rabbit.vhost";
    public final static String HOST_MARKER = "busi.rabbit.host";
    public final static String USER_NAME_MARKER = "busi.rabbit.username";
    public final static String PASSWD_MARKER = "busi.rabbit.passwd";
    public final static String QUEUE_NAME_MARKER = "busi.rabbit.queuename";
    public final static String PORT_MARKER = "busi.rabbit.port";

    public final static String ROUTEKEY_MARKER = "busi.rabbit.route.key";
    public final static String EXCHANGE_MARKER = "busi.rabbit.exchange";
    public final static String EXCHANGETYPE_MARKER = "busi.rabbit.exchange.type";
    public final static String TRANSACTION_MARKER = "busi.rabbit.transaction";
    public final static String DURABLE_MARKER = "busi.rabbit.durable";


    public final static String VHOST_BUYCHART_MARKER = "busi.rabbit.buychart.vhost";
    public final static String HOST_BUYCHART_MARKER = "busi.rabbit.buychart.host";
    public final static String USER_NAME_BUYCHART_MARKER = "busi.rabbit.buychart.username";
    public final static String PASSWD_BUYCHART_MARKER = "busi.rabbit.buychart.passwd";
    public final static String QUEUE_NAME_BUYCHART_MARKER = "busi.rabbit.buychart.queuename";
    public final static String PORT_BUYCHART_MARKER = "busi.rabbit.buychart.port";
    public final static String ROUTEKEY_BUYCHART_MARKER = "busi.rabbit.buychart.route.key";
    public static final String EXCHANGE_BUYCHART_MARKER = "busi.rabbit.buychart.exchange";
    public static final String EXCHANGE_TYPE_BUYCHART_MARKER = "busi.rabbit.buychart.exchange.type";

    public final static String VHOST_REFUND_MARKER = "busi.rabbit.refund.vhost";
    public final static String HOST_REFUND_MARKER = "busi.rabbit.refund.host";
    public final static String USER_NAME_REFUND_MARKER = "busi.rabbit.refund.username";
    public final static String PASSWD_REFUND_MARKER = "busi.rabbit.refund.passwd";
    public final static String QUEUE_NAME_REFUND_MARKER = "busi.rabbit.refund.queuename";
    public final static String PORT_REFUND_MARKER = "busi.rabbit.refund.port";
    public final static String ROUTEKEY_REFUND_MARKER = "busi.rabbit.refund.route.key";
    public static final String EXCHANGE_REFUND_MARKER = "busi.rabbit.refund.exchange";
    public static final String EXCHANGE_TYPE_REFUND_MARKER = "busi.rabbit.refund.exchange.type";

}

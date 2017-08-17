package com.di.mesa.plugin.rabbitmq.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.di.mesa.plugin.rabbitmq.RabbitmqConfigure;
import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by davi on 17/7/31.
 */
public class RabbitMQSpout extends BaseRichSpout {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMQSpout.class);
    private SpoutOutputCollector _collector;
    private String taskId;

    private String queueName = null;
    private String host = null;
    private String userName = null;
    private String password = null;
    private String vhost = null;
    private int port;
    private String routeKey;

    private String exchange;
    private String exchangeType;
    private boolean transaction;
    private boolean durable;

    private String busiMsgType;

    private AtomicBoolean hasSubscribed = new AtomicBoolean(false);

    private ConnectionFactory factory;
    private Connection connection;
    private Channel channel;
    private AtomicBoolean isReady = new AtomicBoolean(false);


    //metric
    private final String TupleCount = "TupleCount";
    private final String EmitCost = "EmitCost";
    private AtomicLong lastTime = new AtomicLong(0L);
    private AtomicLong lastPrintTime = new AtomicLong(0L);
    private ConcurrentHashMap<String, AtomicLong> meticCounter = null;


    public RabbitMQSpout() {
    }


    public RabbitMQSpout(String vhost, String queueName, String host, String userName, String password,String port) {
        this.vhost = vhost;
        this.queueName = queueName;
        this.host = host;
        this.userName = userName;
        this.password = password;
        this.port = Integer.valueOf(port);

        this.busiMsgType = "default";
    }

    public RabbitMQSpout(String vhost, String queueName, String host, String userName, String password, String port, String routeKey, String busiMsgType) {
        this.vhost = vhost;
        this.queueName = queueName;
        this.host = host;
        this.userName = userName;
        this.password = password;
        this.port = Integer.valueOf(port);
        this.routeKey = routeKey;
        this.busiMsgType = busiMsgType;
    }



    public RabbitMQSpout(String busiMsgType) {
        this.busiMsgType = busiMsgType;


    }

    protected String String(Object o) {
        return o.toString();
    }

    public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
        this._collector = collector;
        taskId = context.getThisComponentId();

        isReady.set(openRabbitMQChannel());
        meticCounter = new ConcurrentHashMap();
        logger.info("task {} is ready : {}", taskId, isReady);
    }

    private boolean openRabbitMQChannel() {
        boolean isReady = false;
        try {
            factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(port);
            factory.setUsername(userName);
            factory.setPassword(password);
            factory.setVirtualHost(vhost);

            connection = factory.newConnection();
            channel = connection.createChannel();

            /**
             * 声明要消费的queue。可能消费都先被执行，在消费消息之前要确保queue存在。
             * RabbitmqConfigure.DURABLE 设置消息持久化，设置了持久化也不能保证持久化
             *
             */
            channel.queueDeclare(queueName, RabbitmqConfigure.DURABLE, RabbitmqConfigure.EXCLUSIVE, RabbitmqConfigure.AUTOD_ELETE, null);
            channel.basicQos(1);
            isReady = true;
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            isReady = false;
            closeChannel();
        } catch (TimeoutException e) {
            logger.error(e.getMessage(), e);
            isReady = false;
            closeChannel();
        }

        return isReady;
    }

    private void closeChannel() {
        try {
            if (channel != null)
                channel.close();

            if (connection != null)
                connection.close();
        } catch (IOException e1) {
            logger.error(e1.getMessage(), e1);
        } catch (TimeoutException e1) {
            logger.error(e1.getMessage(), e1);
        }
    }

    public void nextTuple() {
        if (isReady.get()) {

            if (hasSubscribed.get()) {
                try {
                    Thread.sleep(30 * 1000l);
                } catch (InterruptedException e) {
                }
            } else {
                try {
                    subscribe();
                    hasSubscribed.set(true);
                } catch (Exception e) {
                    logger.error("subscribe error , " + e.getMessage(), e);
                    closeChannel();
                    hasSubscribed.set(false);
                    isReady.set(false);
                }
            }
        } else {
            isReady.set(openRabbitMQChannel());
        }
    }

    private void subscribe() throws IOException {
        final Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                //count & cost
                lastTime.set(System.currentTimeMillis());
                recordCounter(meticCounter, TupleCount);
                String message = new String(body, "UTF-8");
                _collector.emit(busiMsgType, new Values(new Object[]{message}));
                channel.basicAck(envelope.getDeliveryTag(), false);
                recordCounter(meticCounter, EmitCost, (System.currentTimeMillis() - lastTime.get()));
                recordMonitorLog();
            }
        };

        channel.basicConsume(queueName, RabbitmqConfigure.AUTOACK, consumer);  //设置自动确认为false
    }


    private void recordMonitorLog() {
        long timeSpan = System.currentTimeMillis() - this.lastPrintTime.get();
        if (timeSpan > 60000L) {
            logger.info("taskIndex {} , Metic_Info {} , Time_Span {}", taskId, this.meticCounter.toString(), timeSpan);
            this.meticCounter.clear();
            this.lastPrintTime.set(System.currentTimeMillis());
        }
    }

    private void recordCounter(ConcurrentHashMap<String, AtomicLong> monitorCounter, String metricMonitor) {
        if (!monitorCounter.containsKey(metricMonitor)) {
            monitorCounter.put(metricMonitor, new AtomicLong(0L));
        }
        ((AtomicLong) monitorCounter.get(metricMonitor)).getAndIncrement();
    }

    private void recordCounter(ConcurrentHashMap<String, AtomicLong> monitorCounter, String metricMonitor, long total) {
        if (!monitorCounter.containsKey(metricMonitor)) {
            monitorCounter.put(metricMonitor, new AtomicLong(0L));
        }
        ((AtomicLong) monitorCounter.get(metricMonitor)).getAndAdd(total);
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(busiMsgType, new Fields(new String[]{"vcontent"}));
    }
}

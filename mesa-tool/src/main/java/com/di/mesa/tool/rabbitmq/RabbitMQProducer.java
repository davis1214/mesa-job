package com.di.mesa.tool.rabbitmq;


import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import com.weidian.di.storm.mesa.storm.constant.RabbitmqConfigure;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeoutException;


/**
 * Created by davi on 17/7/31.
 */
public class RabbitMQProducer {


    private final static String QUEUE_NAME = "q.di.hejian";

    public static void main(String[] args) throws IOException, TimeoutException {
        // 创建连接和频道
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("10.1.16.61");
        // factory.setPort(15672);
        factory.setUsername("test");
        factory.setPassword("test123456");

        factory.setVirtualHost("/settle_test");


        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        //String queue, boolean durable, boolean exclusive, boolean autoDelete,Map<String, Object> arguments
        // channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        channel.queueDeclare(QUEUE_NAME, RabbitmqConfigure.DURABLE, RabbitmqConfigure.EXCLUSIVE, RabbitmqConfigure.AUTOD_ELETE, null);


        String basiMq = "3669 -> msg->{\"orderInfoDO\":{\"add_time\":\"2017-07-31 15:02:22\",\"buyer_id\":\"161756599\",\"express_fee\":0,\"extend_info\":{\"source\":\"DETAIL\"," +
                "\"cnl\":\"fx\",\"v_a_id\":\"h5\"},\"f_seller_id\":400268078,\"fx_fee\":1,\"order_flags\":[14],\"order_id\":800012142342516,\"order_source\":0,\"order_status\":60,\"order_type\":3,\"seller_id\":400263008,\"total_fee\":0,\"total_price\":10},\"subOrderInfoList\":[{\"add_time\":\"2017-07-31 15:02:23\",\"extend_info\":{\"stock_id\":\"2466870726135\"},\"id\":600007195054452,\"item_id\":1149296119,\"item_sku_id\":0,\"item_title\":\"test\",\"price\":10,\"quantity\":1,\"status\":0,\"total_price\":10}]}";


        long n = Long.MAX_VALUE;
        for (int i = 0; i < n; i++) {
            String message = "count " + i + " ---> " + new Date().toLocaleString() + " : " + basiMq;

            // 往转发器上发送消息
            // channel.basicPublish(QUEUE_NAME, "", null, message.getBytes());

            // channel.basicPublish(QUEUE_NAME, QUEUE_NAME, null, message.getBytes("UTF-8"));

            //发送消息，设置消息为持久化消息：MessageProperties.PERSISTENT_TEXT_PLAIN（deliveryMode=2）
            channel.basicPublish("", QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());

            System.out.println(" [x] Sent '" + message + "'");

            try {
                Thread.sleep(10l);
            } catch (InterruptedException e) {
            }
        }

        channel.close();
        connection.close();

    }
}
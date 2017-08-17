package com.di.mesa.tool.rabbitmq;


import com.di.mesa.plugin.rabbitmq.RabbitmqConfigure;
import com.rabbitmq.client.*;

import java.io.IOException;


/**
 * Created by davi on 17/7/31.
 */
public class RabbitMQConsumer {
    private final static String QUEUE_NAME = "q.di.hejian";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("10.1.16.61");
        // factory.setPort(15672);
        factory.setUsername("test");
        factory.setPassword("test123456");

        factory.setVirtualHost("/settle_test");

        Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();

        /**
         * 声明要消费的queue。可能消费都先被执行，在消费消息之前要确保queue存在。
         * RabbitmqConfigure.DURABLE 设置消息持久化，设置了持久化也不能保证持久化
         *
         */
        channel.queueDeclare(QUEUE_NAME, RabbitmqConfigure.DURABLE, RabbitmqConfigure.EXCLUSIVE, RabbitmqConfigure.AUTOD_ELETE, null);

        channel.basicQos(1);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");


        final Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println("Received '" + message + "'");
                try {
                    Thread.sleep(10l);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    System.out.println("basicAck  Done");
                    //手动确认
                    channel.basicAck(envelope.getDeliveryTag(), false);
                }
            }
        };

        //设置自动确认为false
        channel.basicConsume(QUEUE_NAME, RabbitmqConfigure.AUTOACK, consumer);
    }


}
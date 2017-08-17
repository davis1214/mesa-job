package com.di.mesa.tool.kafka;


import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class ProducerExample {
    private static final String BROKER_LIST = "10.1.24.100:9092,10.1.24.101:9092,10.1.24.102:9092"; // broker的地址和端口
    private static final String SERIALIZER_CLASS = "kafka.serializer.StringEncoder"; // 序列化类

    public static void main(String[] args) {

        String TOPIC = "test";

        String schemaString = "{\"namespace\": \"example.avro\", \"type\": \"record\", " + "\"name\": \"page_visit\","
                + "\"fields\": [" + "{\"name\": \"time\", \"type\": \"long\"},"
                + "{\"name\": \"site\", \"type\": \"string\"}," + "{\"name\": \"ip\", \"type\": \"string\"}" + "]}";

        Properties props = new Properties();
        props.put("serializer.class", SERIALIZER_CLASS);
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "-1");
        props.put("metadata.broker.list", BROKER_LIST);

        ProducerConfig config = new ProducerConfig(props);

        Producer<String, String> producer = new Producer<String, String>(config);

        Random rnd = new Random();
        for (long nEvents = 0; nEvents < 100; nEvents++) {
            long runtime = new Date().getTime();

            KeyedMessage<String, String> message = new KeyedMessage<String, String>(TOPIC, schemaString);
            producer.send(message);

        }
        producer.close();
    }
}

package com.testStockStreams;

import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.*;

class StreamsService {

    private final static String BOOTSTRAP_SERVER = "localhost:9092";
    private final static String ZK_SERVER = "localhost:2181";

    static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG , "KafkaProducer");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG , BOOTSTRAP_SERVER);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG , "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG , Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG , Serdes.Double().getClass());

        return properties;
    }

    static void createTopic(String topicName) {
        AdminClient admin = AdminClient.create(initConfig());

        ZkClient zkClient = new ZkClient(ZK_SERVER, 10000, 10000, ZKStringSerializer$.MODULE$);
        admin.createTopics(Collections.singletonList(new NewTopic(topicName, 10, (short) 10).configs(new HashMap<>())));
        zkClient.close();
    }
}

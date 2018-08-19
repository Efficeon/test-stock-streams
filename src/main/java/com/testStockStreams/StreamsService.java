package com.testStockStreams;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.*;

class StreamsService {

    private final static String BOOTSTRAP_SERVER = "localhost:9092";

    static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG , "KafkaStreams");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG , BOOTSTRAP_SERVER);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG , Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG , Serdes.Double().getClass());

        return properties;
    }

    static void createTopic(String topicName) {
        AdminClient admin = AdminClient.create(initConfig());
        admin.createTopics(Collections.singletonList(new NewTopic(topicName, 1, (short) 1).configs(new HashMap<>())));
    }
}

package com.testStockStreams;


import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;


import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

class PropertiesConfig {

    private final static String BOOTSTRAP_SERVER = "localhost:9092";
    private final static String ZK_SERVER = "localhost:2181";

    static Properties initConfig() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG , "KafkaProducer");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG , BOOTSTRAP_SERVER);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG , "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG , Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG , Serdes.Double().getClass());

        return config;
    }

    static void createTopic(String topicName) {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);

        AdminClient admin = AdminClient.create(config);

        Map<String, String> configs = new HashMap<>();
        int partitions = 10;
        short replication = 10;

        ZkClient zkClient = null;
            admin.createTopics(Arrays.asList(new NewTopic(topicName, partitions, replication).configs(configs)));

            zkClient = new ZkClient(ZK_SERVER, 10000, 10000, ZKStringSerializer$.MODULE$);
            zkClient.close();
    }
}

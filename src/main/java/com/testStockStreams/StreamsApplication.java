package com.testStockStreams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

public class StreamsApplication {

    private final static String TOPIC_MASTER = "stock-topic-master";
    private final static String TOPIC_RECENT_DATA = "stock-topic-recent-data";
    private final static String TOPIC_LESS = "stock-topic-less";
    private final static String TOPIC_MORE = "stock-topic-more";
    private final static String TOPIC_AVERAGES_STATISTICAL = "stock-topic-averages";

    public static void main(String[] args) {
        Properties config = StreamsService.initConfig();
        StreamsService.createTopic(TOPIC_MASTER);
        StreamsService.createTopic(TOPIC_RECENT_DATA);
        StreamsService.createTopic(TOPIC_LESS);
        StreamsService.createTopic(TOPIC_MORE);
        StreamsService.createTopic(TOPIC_AVERAGES_STATISTICAL);

        KafkaStreams streams = new KafkaStreams(createTopology(), config);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public static Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Double> quotes = builder.stream(TOPIC_MASTER);

        /* save latest data on quotes */
        KTable<String, Double> quotesSlice = quotes.groupByKey().reduce((previousValue, newValue) -> newValue);

        /* save average value of quotations in KTable */
        KTable<String, Long> countValues = quotes.groupByKey().count();
        KTable<String, Double> sumValues = quotes.groupByKey().reduce((v1, v2) -> v1 + v2);
        KTable<String, Double> averages = sumValues.join(countValues, (sum, count) -> sum/count.doubleValue());

        /* tables (1) and (2) need to be additionally merged into two topics in Kafka */
        averages.to(Serdes.String(), Serdes.Double(), TOPIC_AVERAGES_STATISTICAL);
        quotesSlice.to(Serdes.String(), Serdes.Double(), TOPIC_RECENT_DATA);

        /* split the input stream of quotations into the other two and save them in two different topics,
           one where the value of the quotes> = 2.5, in another <2.5 */
        KStream<String, Double> valuesLess = quotes.filter((key, value) -> value < 2.5);
        valuesLess.to(Serdes.String(), Serdes.Double(), TOPIC_LESS);
        KStream<String, Double> valuesMore = quotes.filter((key, value) -> value >= 2.5);
        valuesMore.to(Serdes.String(), Serdes.Double(), TOPIC_MORE);

        return builder.build();
    }
}

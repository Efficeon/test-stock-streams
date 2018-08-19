import com.testStockStreams.StreamsApplication;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertNull;

public class StreamsApplicationTest {

    private StringSerializer stringSerializer = new StringSerializer();
    private DoubleSerializer doubleSerializer = new DoubleSerializer();

    private ConsumerRecordFactory<String, Double> recordFactory = new ConsumerRecordFactory<>(stringSerializer, doubleSerializer);

    private TopologyTestDriver testDriver;

    @Before
    public void setUpTopologyTestDriver() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG , "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG , "testhost:1234");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG , "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG , Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG , Serdes.Double().getClass());

        StreamsApplication streamsApp = new StreamsApplication();
        Topology topology = streamsApp.createTopology();
        testDriver = new TopologyTestDriver(topology, config);
    }

    private void pushData(String key, Double value) {
        testDriver.pipeInput(recordFactory.create("stock-topic-master", key, value));
    }

    private ProducerRecord<String, Double> readOutputLastData() {
        return testDriver.readOutput("stock-topic-last-data", new StringDeserializer(), new DoubleDeserializer());
    }

    private ProducerRecord<String, Double> readOutputLess() {
        return testDriver.readOutput("stock-topic-less", new StringDeserializer(), new DoubleDeserializer());
    }

    private ProducerRecord<String, Double> readOutputMore() {
        return testDriver.readOutput("stock-topic-more", new StringDeserializer(), new DoubleDeserializer());
    }

    private ProducerRecord<String, Double> readOutputAverages() {
        return testDriver.readOutput("stock-topic-averages", new StringDeserializer(), new DoubleDeserializer());
    }

    @Test
    public void lastDataTest(){
        pushData("GOOG", 3.0);
        OutputVerifier.compareKeyValue(readOutputLastData(), "GOOG", 3.0);
        assertNull(readOutputLastData());

        pushData("GOOG", 2.4);
        OutputVerifier.compareKeyValue(readOutputLastData(), "GOOG", 2.4);
        assertNull(readOutputLastData());

    }

    @Test
    public void moreDataTest(){
        pushData("GOOG", 3.0);
        OutputVerifier.compareKeyValue(readOutputMore(), "GOOG", 3.0);
        assertNull(readOutputMore());

        pushData("GOOG", 2.4);
        assertNull(readOutputMore());
    }

    @Test
    public void lessDataTest(){
        pushData("GOOG", 3.0);
        assertNull(readOutputLess());

        pushData("GOOG", 2.4);
        OutputVerifier.compareKeyValue(readOutputLess(), "GOOG", 2.4);
        assertNull(readOutputLess());
    }

    @Test
    public void averagesDataTest() {

        pushData("GOOG", 3.0);
        OutputVerifier.compareKeyValue(readOutputAverages(), "GOOG", 3.0);
        OutputVerifier.compareKeyValue(readOutputAverages(), "GOOG", 3.0);
        assertNull(readOutputAverages());

        pushData("GOOG", 2.4);
        OutputVerifier.compareKeyValue(readOutputAverages(), "GOOG", 2.7);
        OutputVerifier.compareKeyValue(readOutputAverages(), "GOOG", 2.7);
        assertNull(readOutputAverages());
    }

    @After
    public void closeTestDriver() {
        testDriver.close();
    }
}
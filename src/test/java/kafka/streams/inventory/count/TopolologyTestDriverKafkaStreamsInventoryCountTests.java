package kafka.streams.inventory.count;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Properties;
import kafka.streams.inventory.count.KafkaStreamsInventoryCountApplication.KafkaStreamsInventoryAggregator;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonSerde;

import static kafka.streams.inventory.count.KafkaStreamsInventoryCountApplication.STORE_NAME;

public class TopolologyTestDriverKafkaStreamsInventoryCountTests {

    static final String INPUT_TOPIC = "inventory-update-events";
    static final String OUTPUT_TOPIC = "inventory-count-events";
    private static final String GROUP_NAME = "inventory-count-test";

    private final ObjectMapper mapper = new ObjectMapper();

    private Serde<InventoryCountEvent> countEventSerde = new JsonSerde<>(InventoryCountEvent.class, mapper);
    private Serde<InventoryUpdateEvent> updateEventSerde = new JsonSerde<>(InventoryUpdateEvent.class, mapper);
    private Serde<ProductKey> keySerde = new JsonSerde<>(ProductKey.class);

    private ConsumerRecordFactory<ProductKey, InventoryUpdateEvent> updateEventConsumerRecordFactory = new ConsumerRecordFactory<>(
            keySerde.serializer(), updateEventSerde.serializer());


    private TopologyTestDriver testDriver;

    static Properties getStreamsConfiguration() {
        final Properties streamsConfiguration = new Properties();
        // Need to be set even these do not matter with TopologyTestDriver
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "TopologyTestDriver");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        return streamsConfiguration;
    }


    @BeforeEach
     void setup() {
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<ProductKey, InventoryUpdateEvent> input = builder.stream(INPUT_TOPIC, Consumed.with(keySerde, updateEventSerde));
        KafkaStreamsInventoryAggregator app = new KafkaStreamsInventoryAggregator(Stores.inMemoryKeyValueStore(STORE_NAME));
        KStream<ProductKey, InventoryCountEvent> output = app.process(input);
        output.to(OUTPUT_TOPIC, Produced.with(keySerde, countEventSerde));
        testDriver = new TopologyTestDriver(builder.build(), getStreamsConfiguration());
    }

    @AfterEach
    void tearDown() {
        try {
            testDriver.close();
        } catch (final RuntimeException e) {
            // https://issues.apache.org/jira/browse/KAFKA-6647 causes exception when executed in Windows, ignoring it
            // Logged stacktrace cannot be avoided
            System.out.println("Ignoring exception, test failing in Windows due this exception:" + e.getLocalizedMessage());
        }
    }

    @Test
    void test() {

    }

    /**
     * Read one Record from output topic.
     *
     * @return ProducerRecord containing WordCount as value
     */
    private ProducerRecord<ProductKey, InventoryCountEvent> readOutput() {
        return testDriver.readOutput(OUTPUT_TOPIC, keySerde.deserializer(), countEventSerde.deserializer());
    }


}

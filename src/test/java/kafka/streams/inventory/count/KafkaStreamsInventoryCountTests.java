package kafka.streams.inventory.count;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

public class KafkaStreamsInventoryCountTests {

    private static ObjectMapper mapper = new ObjectMapper();
    private static Serde<InventoryUpdateEvent> domainEventSerde = new JsonSerde<>(InventoryUpdateEvent.class, mapper);
    private static Serde<InventoryCountEvent> summaryEventSerde = new JsonSerde<>(InventoryCountEvent.class, mapper);
    private static Serde<ProductKey> keySerde = new JsonSerde<>(ProductKey.class, mapper);

    private static final String INPUT_TOPIC = "inventory-update-events";
    private static final String OUTPUT_TOPIC = "inventory-count-events";
    private static final String GROUP_NAME = "inventory-count-test";

    private Consumer<ProductKey, InventoryCountEvent> consumer;

    private static InventoryUpdateEventGenerator eventGenerator;

    private static ConfigurableApplicationContext context;
    private static DefaultKafkaConsumerFactory<ProductKey, InventoryCountEvent> cf;

    @ClassRule
    public static EmbeddedKafkaRule embeddedKafka = new EmbeddedKafkaRule(1, false, INPUT_TOPIC);

    @BeforeClass
    public static void init() {
        Map<String, Object> props = new HashMap<>();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getEmbeddedKafka().getBrokersAsString());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerde.serializer().getClass());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, domainEventSerde.serializer().getClass());
        eventGenerator = new InventoryUpdateEventGenerator(props, INPUT_TOPIC);

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(GROUP_NAME, "true", embeddedKafka.getEmbeddedKafka());
        consumerProps.put("key.deserializer", keySerde.deserializer().getClass());
        consumerProps.put("value.deserializer", summaryEventSerde.deserializer().getClass());
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "test");
        consumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, KafkaStreamsInventoryCountTests.class.getPackage().getName());
        consumerProps.put(JsonDeserializer.KEY_DEFAULT_TYPE, ProductKey.class);
        consumerProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, InventoryCountEvent.class);
        consumerProps.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, "false");
        cf = new DefaultKafkaConsumerFactory<>(consumerProps);


        context = new SpringApplicationBuilder(KafkaStreamsInventoryCountApplication.class)
                .properties(
                        "logger.level.kafka.streams.table.join=DEBUG",
                        "spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getEmbeddedKafka().getBrokersAsString(),
                        "spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000")
                .run();

    }

    @AfterClass
    public static void shutdown() {
        context.close();
    }


    @Before
    public void setUp() {
        eventGenerator.reset();
        consumer = cf.createConsumer(UUID.randomUUID().toString());
        consumer.subscribe(Collections.singleton(OUTPUT_TOPIC));
    }

    @After
    public void cleanup() {
        consumer.close();
    }


    @Test
    public void processMessagesForSingleKey() {

        Map<ProductKey, Integer> expectedCounts = eventGenerator.generateRandomMessages(1, 3);

        Map<ProductKey, InventoryCountEvent> actualEvents = consumeActualEvents(1);

        assertThat(actualEvents).hasSize(1);

        ProductKey key = actualEvents.keySet().iterator().next();

        assertThat(actualEvents.get(key).getCount()).isEqualTo(expectedCounts.get(key));

    }

    @Test
    public void processAccumulatedMessagesForSingleKey() {
        Map<ProductKey, Integer> expectedCounts;
        expectedCounts = eventGenerator.generateRandomMessages(1, 20);
        Map<ProductKey, InventoryCountEvent> actualEvents = consumeActualEvents(1);

        assertThat(actualEvents).hasSize(1);

        ProductKey key = actualEvents.keySet().iterator().next();

        assertThat(actualEvents.get(key).getCount()).isEqualTo(expectedCounts.get(key));

        expectedCounts = eventGenerator.generateRandomMessages(1, 20);

        Map<ProductKey, InventoryCountEvent> actualEvents2 = consumeActualEvents(1);

        assertThat(actualEvents2.get(key).getCount()).isEqualTo(expectedCounts.get(key));


    }

    @Test
    public void processAccumulatedMessagesForMultipleKeys() {
        Map<ProductKey, Integer> balances1 = eventGenerator.generateRandomMessages(10, 5);

        Map<ProductKey, InventoryCountEvent> expectedEvents;
        expectedEvents = consumeActualEvents(10);
        assertThat(expectedEvents).hasSize(10);

        for (ProductKey key : balances1.keySet()) {
            assertThat(expectedEvents.get(key).getCount()).isEqualTo(balances1.get(key));
        }

        Map<ProductKey, Integer> balances2 = eventGenerator.generateRandomMessages(10, 5);

        expectedEvents = consumeActualEvents(10);
        assertThat(expectedEvents).hasSize(10);

        boolean atLeastOneOriginalBalanceIsDifferent = false;

        for (ProductKey key : balances2.keySet()) {
            assertThat(expectedEvents.get(key).getCount()).isEqualTo(balances2.get(key));
            atLeastOneOriginalBalanceIsDifferent = atLeastOneOriginalBalanceIsDifferent || !balances1.get(key).equals(balances2.get(key));
        }

        //Verify that the expected counts changed from the first round of events.
        assertThat(atLeastOneOriginalBalanceIsDifferent).isTrue();
    }

    private Map<ProductKey, InventoryCountEvent> consumeActualEvents(int count) {
        Map<ProductKey, InventoryCountEvent> summaryEvents = new LinkedHashMap<>();
        while (summaryEvents.size() < count) {
            ConsumerRecords<ProductKey, InventoryCountEvent> records = KafkaTestUtils.getRecords(consumer, 5000);
            if (records.isEmpty()) {
                break;
            }
            for (Iterator<ConsumerRecord<ProductKey, InventoryCountEvent>> it = records.iterator(); it.hasNext(); ) {
                ConsumerRecord<ProductKey, InventoryCountEvent> consumerRecord = it.next();
                summaryEvents.put(consumerRecord.key(), consumerRecord.value());
            }
        }
        return summaryEvents;

    }

}

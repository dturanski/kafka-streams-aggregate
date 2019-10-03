/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keySerde.deserializer().getClass());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, summaryEventSerde.deserializer().getClass());
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "test");
        consumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, KafkaStreamsInventoryCountTests.class.getPackage().getName());
        consumerProps.put(JsonDeserializer.KEY_DEFAULT_TYPE, ProductKey.class);
        consumerProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, InventoryCountEvent.class);
        consumerProps.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, "false");
        cf = new DefaultKafkaConsumerFactory<>(consumerProps);


        context = new SpringApplicationBuilder(KafkaStreamsInventoryCountApplication.class)
                .properties(
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

        Map<ProductKey, Integer> expectedCounts = eventGenerator.generateRandomEvents(1, 3);

        Map<ProductKey, InventoryCountEvent> actualEvents = consumeActualInventoryCountEvents(1);

        assertThat(actualEvents).hasSize(1);

        ProductKey key = actualEvents.keySet().iterator().next();

        assertThat(actualEvents.get(key).getCount()).isEqualTo(expectedCounts.get(key));

    }

    @Test
    public void processAggregatedEventsForSingleKey() {
        Map<ProductKey, Integer> expectedCount;
        expectedCount = eventGenerator.generateRandomEvents(1, 5);
        Map<ProductKey, InventoryCountEvent> originalCount = consumeActualInventoryCountEvents(1);

        assertThat(originalCount).hasSize(1);

        ProductKey key = originalCount.keySet().iterator().next();

        assertThat(originalCount.get(key).getCount()).isEqualTo(expectedCount.get(key));

        expectedCount = eventGenerator.generateRandomEvents(1, 5);

        Map<ProductKey, InventoryCountEvent> actualCount = consumeActualInventoryCountEvents(1);

        assertThat(actualCount.get(key).getCount()).isEqualTo(expectedCount.get(key));


    }

    @Test
    public void processAggregatedEventsForMultipleKeys() {
        Map<ProductKey, Integer> initialCounts = eventGenerator.generateRandomEvents(10, 5);

        Map<ProductKey, InventoryCountEvent> expectedEvents;
        expectedEvents = consumeActualInventoryCountEvents(10);
        assertThat(expectedEvents).hasSize(10);

        for (ProductKey key : initialCounts.keySet()) {
            assertThat(expectedEvents.get(key).getCount()).isEqualTo(initialCounts.get(key));
        }

        Map<ProductKey, Integer> updatedCounts = eventGenerator.generateRandomEvents(10, 5);

        expectedEvents = consumeActualInventoryCountEvents(10);
        assertThat(expectedEvents).hasSize(10);

        boolean atLeastOneUpdatedCountIsDifferent = false;

        for (ProductKey key : updatedCounts.keySet()) {
            assertThat(expectedEvents.get(key).getCount()).isEqualTo(updatedCounts.get(key));
            atLeastOneUpdatedCountIsDifferent = atLeastOneUpdatedCountIsDifferent || !initialCounts.get(key).equals(updatedCounts.get(key));
        }

        //Verify that the expected counts changed from the first round of events.
        assertThat(atLeastOneUpdatedCountIsDifferent).isTrue();
    }

    private Map<ProductKey, InventoryCountEvent> consumeActualInventoryCountEvents(int count) {
        Map<ProductKey, InventoryCountEvent> inventoryCountEvents = new LinkedHashMap<>();
        while (inventoryCountEvents.size() < count) {
            ConsumerRecords<ProductKey, InventoryCountEvent> records = KafkaTestUtils.getRecords(consumer, 5000);
            if (records.isEmpty()) {
                break;
            }
            for (Iterator<ConsumerRecord<ProductKey, InventoryCountEvent>> it = records.iterator(); it.hasNext(); ) {
                ConsumerRecord<ProductKey, InventoryCountEvent> consumerRecord = it.next();
                inventoryCountEvents.put(consumerRecord.key(), consumerRecord.value());
            }
        }
        return inventoryCountEvents;

    }

}

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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

@EmbeddedKafka(topics = KafkaStreamsInventoryCountTests.INPUT_TOPIC)
public class KafkaStreamsInventoryCountTests {

    static final String INPUT_TOPIC = "inventory-update-events";
    static final String OUTPUT_TOPIC = "inventory-count-events";
    private static final String GROUP_NAME = "inventory-count-test";

    private Consumer<ProductKey, InventoryCountEvent> consumer;

    private static InventoryUpdateEventGenerator eventGenerator;

    private static ConfigurableApplicationContext context;
    private static DefaultKafkaConsumerFactory<ProductKey, InventoryCountEvent> cf;
    private static StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @BeforeAll
    public static void init(EmbeddedKafkaBroker embeddedKafka) {
        Map<String, Object> props = new HashMap<>();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        eventGenerator = new InventoryUpdateEventGenerator(props, INPUT_TOPIC);

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(GROUP_NAME, "true", embeddedKafka);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "test");
        consumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, KafkaStreamsInventoryCountTests.class.getPackage().getName());
        consumerProps.put(JsonDeserializer.KEY_DEFAULT_TYPE, ProductKey.class);
        consumerProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, InventoryCountEvent.class);
        consumerProps.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, "false");
        cf = new DefaultKafkaConsumerFactory<>(consumerProps);


        context = new SpringApplicationBuilder(KafkaStreamsInventoryCountApplication.class)
                .properties(
                        "spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString(),
                        "spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000")
                .run();
        streamsBuilderFactoryBean = context.getBean(StreamsBuilderFactoryBean.class);

    }

    @AfterAll
    public static void shutdown() {
        context.close();
    }


    @BeforeEach
    public void setUp() {
        eventGenerator.reset();
        consumer = cf.createConsumer(UUID.randomUUID().toString());
        consumer.subscribe(Collections.singleton(OUTPUT_TOPIC));
    }

    @AfterEach
    public void cleanup() {
        consumer.close();
    }


    @RepeatedTest(10)
    public void processMessagesForSingleKey() {

        Map<ProductKey, InventoryCountEvent> expectedCounts = eventGenerator.generateRandomEvents(1, 3);

        Map<ProductKey, InventoryCountEvent> actualEvents = consumeActualInventoryCountEvents(1);

        assertThat(actualEvents).hasSize(1);

        ProductKey key = actualEvents.keySet().iterator().next();

        assertThat(actualEvents.get(key).getCount()).isEqualTo(expectedCounts.get(key).getCount());

    }

    @RepeatedTest(10)
    public void processAggregatedEventsForSingleKey() {
        Map<ProductKey, InventoryCountEvent> expectedCount;
        expectedCount = eventGenerator.generateRandomEvents(1, 5);
        Map<ProductKey, InventoryCountEvent> originalCount = consumeActualInventoryCountEvents(1);

        assertThat(originalCount).hasSize(1);

        ProductKey key = originalCount.keySet().iterator().next();

        assertThat(originalCount.get(key).getCount()).isEqualTo(expectedCount.get(key).getCount());

        expectedCount = eventGenerator.generateRandomEvents(1, 5);

        Map<ProductKey, InventoryCountEvent> actualCount = consumeActualInventoryCountEvents(1);

        assertThat(actualCount.get(key).getCount()).isEqualTo(expectedCount.get(key).getCount());


    }

    @RepeatedTest(10)
    public void processAggregatedEventsForMultipleKeys() {
        Map<ProductKey, InventoryCountEvent> initialCounts = eventGenerator.generateRandomEvents(10, 5);

        Map<ProductKey, InventoryCountEvent> expectedEvents;
        expectedEvents = consumeActualInventoryCountEvents(10);
        assertThat(expectedEvents).hasSize(10);

        for (ProductKey key : initialCounts.keySet()) {
            assertThat(expectedEvents.get(key).getCount()).isEqualTo(initialCounts.get(key).getCount());
        }

        Map<ProductKey, InventoryCountEvent> updatedCounts = eventGenerator.generateRandomEvents(10, 5);

        expectedEvents = consumeActualInventoryCountEvents(10);
        assertThat(expectedEvents).hasSize(10);

        boolean atLeastOneUpdatedCountIsDifferent = false;

        for (ProductKey key : updatedCounts.keySet()) {
            assertThat(expectedEvents.get(key).getCount()).isEqualTo(updatedCounts.get(key).getCount());
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

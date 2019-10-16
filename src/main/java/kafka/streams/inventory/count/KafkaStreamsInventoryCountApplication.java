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
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.messaging.handler.annotation.SendTo;


@SpringBootApplication
public class KafkaStreamsInventoryCountApplication {


    final static String STORE_NAME = "inventory-counts";

    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamsInventoryAggregator.class, args);
    }

    @Bean
    public KeyValueBytesStoreSupplier storeSupplier() {
        return Stores.inMemoryKeyValueStore(STORE_NAME);
    }

    @EnableBinding(UpdateEventProcessor.class)
    public class KafkaStreamsInventoryAggregator {

        private final Logger logger = LoggerFactory.getLogger(KafkaStreamsInventoryAggregator.class);

        private final KeyValueBytesStoreSupplier storeSupplier;

        private final InventoryCountUpdateEventUpdater inventoryCountUpdateEventUpdater = new InventoryCountUpdateEventUpdater();

        public KafkaStreamsInventoryAggregator(KeyValueBytesStoreSupplier storeSupplier) {
            this.storeSupplier = storeSupplier;
        }

        @StreamListener("input")
        @SendTo("output")
        public KStream<ProductKey, InventoryCountEvent> process(KStream<ProductKey, InventoryUpdateEvent> input) {

            ObjectMapper mapper = new ObjectMapper();
            Serde<InventoryCountEvent> summaryEventSerde = new JsonSerde<>(InventoryCountEvent.class, mapper);
            Serde<InventoryUpdateEvent> updateEventSerde = new JsonSerde<>(InventoryUpdateEvent.class, mapper);
            Serde<ProductKey> keySerde = new JsonSerde<>(ProductKey.class);

            return input
                    .peek((k,v)->  logger.debug("Processing inventoryUpdateEvent: key {} delta {} action {}",
                            k.getProductCode() ,v.getDelta(),v.getAction()))
                    .groupByKey(Serialized.with(keySerde, updateEventSerde))
                    .aggregate(InventoryCountEvent::new,
                            (key, updateEvent, summaryEvent) -> inventoryCountUpdateEventUpdater.apply(updateEvent, summaryEvent)
                            , Materialized.<ProductKey, InventoryCountEvent>as(storeSupplier)
                                    .withKeySerde(keySerde)
                                    .withValueSerde(summaryEventSerde))

                    .toStream().peek((k, v) -> logger.debug("aggregated count key {} {}" , k.getProductCode(),v.getCount()));
        }
    }

    interface UpdateEventProcessor {
        @Input("input")
        KStream<?, ?> input();

        @Output("output")
        KStream<?, ?> output();
    }
}

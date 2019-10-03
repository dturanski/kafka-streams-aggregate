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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import static kafka.streams.inventory.count.InventoryUpdateEvent.Action.DEC;
import static kafka.streams.inventory.count.InventoryUpdateEvent.Action.INC;
import static kafka.streams.inventory.count.InventoryUpdateEvent.Action.REP;

/**
 * Test fixture to generate {@link InventoryUpdateEvent}s to a topic and maintain the state of the expected aggregated inventory counts.
 */
class InventoryUpdateEventGenerator {

    private final static Logger logger = LoggerFactory.getLogger(InventoryUpdateEventGenerator.class);

    final KafkaTemplate<ProductKey, InventoryUpdateEvent> kafkaTemplate;

    Map<ProductKey, Integer> accumulatedInventoryCounts = new LinkedHashMap<>();

    InventoryUpdateEventGenerator(Map<String, Object> producerProperties, String destination) {

        DefaultKafkaProducerFactory<ProductKey, InventoryUpdateEvent> pf = new DefaultKafkaProducerFactory(producerProperties);
        kafkaTemplate = new KafkaTemplate<>(pf, true);
        kafkaTemplate.setDefaultTopic(destination);
    }


    Map<ProductKey, Integer> generateRandomMessages(int numberKeys, int eventsPerKey) {
        InventoryUpdateEvent.Action[] actions = {INC, DEC, REP};
        return doGenerateMessages(numberKeys, eventsPerKey, actions, Optional.empty());
    }

    void reset() {
        accumulatedInventoryCounts.keySet().forEach(key -> kafkaTemplate.sendDefault(key, null));
    }

    /**
     * @param numberKeys   number of keys to generate events for
     * @param eventsPerKey number of events per key
     * @param actions      the list of update actions to include
     * @param value        an optional value to set for each event instead of random values.
     * @return expected calculated counts. Accumulates values since last reset to simulate what the aggregator does.
     */
    Map<ProductKey, Integer> doGenerateMessages(int numberKeys, int eventsPerKey, InventoryUpdateEvent.Action[] actions, Optional<Integer> value) {
        Random random = new Random();
        InventoryCountUpdateEventUpdater summaryEventUpdater = new InventoryCountUpdateEventUpdater();


        for (int j = 0; j < numberKeys; j++) {
            ProductKey key = new ProductKey("key" + j);
            InventoryCountEvent inventoryCountEvent = new InventoryCountEvent(key,
                    accumulatedInventoryCounts.containsKey(key) ? accumulatedInventoryCounts.get(key) : 0);
            for (int i = 0; i < eventsPerKey; i++) {
                InventoryUpdateEvent inventoryUpdateEvent = new InventoryUpdateEvent();
                inventoryUpdateEvent.setKey(key);

                inventoryUpdateEvent.setDelta(value.orElse(random.nextInt(10) + 1));
                inventoryUpdateEvent.setAction(actions[random.nextInt(actions.length)]);

                inventoryCountEvent = summaryEventUpdater.apply(inventoryUpdateEvent, inventoryCountEvent);


                logger.debug("Sending inventoryUpdateEvent: key {} delta {} action {}",
                        inventoryUpdateEvent.getKey().getProductCode(), inventoryUpdateEvent.getDelta(), inventoryUpdateEvent.getAction());

                kafkaTemplate.sendDefault(inventoryUpdateEvent.getKey(), inventoryUpdateEvent);

            }
            accumulatedInventoryCounts.put(key, inventoryCountEvent.getCount());
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(accumulatedInventoryCounts));

    }
}

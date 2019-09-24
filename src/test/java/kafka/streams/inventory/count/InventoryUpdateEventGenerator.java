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

/**
 * Test fixture to generate {@link InventoryUpdateEvent}s to a topic and maintain the state of the expected aggregated inventory counts.
 *
 */
class InventoryUpdateEventGenerator {

    private final static Logger logger = LoggerFactory.getLogger(InventoryUpdateEventGenerator.class);

    final KafkaTemplate<ProductKey, InventoryUpdateEvent> kafkaTemplate;
    private final String destination;

    Map<ProductKey, Integer> accumulatedBalances = new LinkedHashMap<>();

    InventoryUpdateEventGenerator(Map<String, Object> producerProperties, String destination) {

        DefaultKafkaProducerFactory<ProductKey, InventoryUpdateEvent> pf = new DefaultKafkaProducerFactory(producerProperties);
        kafkaTemplate = new KafkaTemplate<>(pf, true);
        this.destination = destination;
    }


    Map<ProductKey, Integer> generateRandomMessages(int numberKeys, int eventsPerKey) {
        InventoryUpdateEvent.Action[] actions = {INC, DEC};
        return doGenerateMessages(numberKeys, eventsPerKey, actions, Optional.empty());
    }

    void reset() {
        accumulatedBalances.keySet().forEach(key -> kafkaTemplate.send(destination, key, null));
    }

    /**
     * @param numberKeys   number of keys to generate events for
     * @param eventsPerKey number of events per key
     * @param actions      the list of update actions to include
     * @param value        an optional value to set for each event instead of random values.
     * @return expected calculated balances. Accumulated values since last reset.
     */
    Map<ProductKey, Integer> doGenerateMessages(int numberKeys, int eventsPerKey, InventoryUpdateEvent.Action[] actions, Optional<Integer> value) {
        Random random = new Random();
        InventoryCountUpdateEventUpdater summaryEventUpdater = new InventoryCountUpdateEventUpdater();


        for (int j = 0; j < numberKeys; j++) {
            ProductKey key = new ProductKey("key" + j);
            InventoryCountEvent inventoryCountEvent = new InventoryCountEvent(key,
                    accumulatedBalances.containsKey(key) ? accumulatedBalances.get(key) : 0);
            for (int i = 0; i < eventsPerKey; i++) {
                InventoryUpdateEvent updateEvent = new InventoryUpdateEvent();
                updateEvent.setKey(key);

                updateEvent.setDelta(value.orElse(random.nextInt(10) + 1));
                updateEvent.setAction(actions[random.nextInt(actions.length)]);

                inventoryCountEvent = summaryEventUpdater.apply(updateEvent, inventoryCountEvent);


                logger.debug("Sending InventoryUpdateEvent:" + updateEvent.getKey().getProductCode() + " : " + updateEvent.getDelta() + " " + updateEvent.getAction());

                kafkaTemplate.send(destination, updateEvent.getKey(), updateEvent);

            }
            accumulatedBalances.put(key, inventoryCountEvent.getCount());
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(accumulatedBalances));

    }
}

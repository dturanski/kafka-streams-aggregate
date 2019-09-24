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

import java.util.function.BiFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InventoryCountUpdateEventUpdater implements BiFunction<InventoryUpdateEvent, InventoryCountEvent, InventoryCountEvent> {
    private final static Logger logger = LoggerFactory.getLogger(InventoryCountUpdateEventUpdater.class);

    @Override
    public InventoryCountEvent apply(InventoryUpdateEvent updateEvent, InventoryCountEvent summaryEvent) {
        int delta = updateEvent.getDelta();
        logger.debug("Applying update {} {} {} to summaryEvent. Current count is {}",
                updateEvent.getKey().getProductCode(), updateEvent.getAction(), updateEvent.getDelta(), summaryEvent.getCount());
        switch (updateEvent.getAction()) {
            case DEC:
                summaryEvent.setCount(summaryEvent.getCount() - delta);
                break;
            case INC:
                summaryEvent.setCount(summaryEvent.getCount() + delta);
                break;
            case REP:
                summaryEvent.setCount(delta);
                break;
            default:
                return null;
        }
        logger.debug("Applied update {} {} {} to summaryEvent. Current count is {}",
                updateEvent.getKey().getProductCode(), updateEvent.getAction(), updateEvent.getDelta(), summaryEvent.getCount());
        return summaryEvent;
    }
}
/*
 * Copyright 2018 the original author or authors.
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

package kafka.streams.table.join;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Random;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.HashMap;
import java.util.Map;

import static kafka.streams.table.join.DomainEvent.Action.DEC;

/**
 * @author Soby Chacko
 */
public class Producers {

	public static void main(String... args) {
		Random random = new Random();

		ObjectMapper mapper = new ObjectMapper();
		Serde<DomainEvent> domainEventSerde = new JsonSerde<>(DomainEvent.class, mapper);


		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
		props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, domainEventSerde.serializer().getClass());

		DefaultKafkaProducerFactory<String, DomainEvent> pf = new DefaultKafkaProducerFactory<>(props);
		KafkaTemplate<String, DomainEvent> template = new KafkaTemplate<>(pf, true);
		template.setDefaultTopic("balance-on-hand");
		int[] balances = new int[10];

		for (int j=0; j<10; j++) {
			balances[j] = 0;
			for (int i = 0; i < 10; i++) {
				DomainEvent ddEvent = new DomainEvent();
				ddEvent.setKey("key"+j);
				ddEvent.setDelta(random.nextInt(10)+1);
				ddEvent.setAction(i % 2 == 0 ? DEC : DomainEvent.Action.INC);
				if (ddEvent.getAction()==DEC) {
					balances[j] -= ddEvent.getDelta();
				} else {
					balances[j] += ddEvent.getDelta();
				}

				System.out.println(ddEvent.getKey()+ " : " + ddEvent.getDelta());

				template.sendDefault(ddEvent.getKey(), ddEvent);

			}
		}
		for (int i=0; i < balances.length; i++) {
			System.out.println(String.format("%d : balance %d", i, balances[i]));
		}
	}
}

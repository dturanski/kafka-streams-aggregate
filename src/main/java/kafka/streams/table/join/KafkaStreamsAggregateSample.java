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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import static kafka.streams.table.join.DomainEvent.Action.DEC;

@SpringBootApplication
public class KafkaStreamsAggregateSample {

	@Autowired
	private InteractiveQueryService queryService;

	public static void main(String[] args) {
		SpringApplication.run(KafkaStreamsAggregateSample.class, args);
	}


	final static String STORE_NAME = "balance-on-hand-ktable1";


	@EnableBinding(KafkaStreamsProcessorX.class)
	public class KafkaStreamsAggregateSampleApplication {

		@StreamListener
		public void process(@Input("input") KStream<String, DomainEvent> input, @Input("ktable") KTable<String, Integer> kTable) {

			input.leftJoin(kTable,(domainEvent, balanceOnHand) -> {
				int delta =  balanceOnHand == null ? 0 : balanceOnHand;
				if (domainEvent.getAction().equals(DEC)) {
					delta -= domainEvent.getDelta();
				} else {
					delta += domainEvent.getDelta();
				}
				System.out.println(String.format("key: %s delta: %d", domainEvent.getKey(), delta));
				return delta;
			}).groupByKey(Serialized.with(Serdes.String(), Serdes.Integer()))
		      .aggregate(() -> new Integer(0),
							(key, i1,i2) -> {
								System.out.println(String.format("key: %s i1: %d i2: %d",key,i1, i2));
		      					return i1 + i2;
							}, Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as(STORE_NAME)
							  .withKeySerde(Serdes.String())
							  .withValueSerde(Serdes.Integer()));
		}
	}

	@RestController
	public class BalanceOnHandController {
		ReadOnlyKeyValueStore<String, Integer> balanceStore;

		@RequestMapping("/boh/{key}")
		public Integer balanceOnHand(@PathVariable String key) {
			if (balanceStore == null) {
				balanceStore = queryService.getQueryableStore(STORE_NAME, QueryableStoreTypes.keyValueStore());
			}

			return balanceStore.get(key);
		}
	}

	interface KafkaStreamsProcessorX {

		@Input("input")
		KStream<?, ?> input();

		@Input("ktable")
		KTable<?,?> ktable();
	}
}

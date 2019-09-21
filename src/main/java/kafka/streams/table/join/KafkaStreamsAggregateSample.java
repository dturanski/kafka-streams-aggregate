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
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.messaging.handler.annotation.SendTo;

import static kafka.streams.table.join.DomainEvent.Action.DEC;
import static kafka.streams.table.join.DomainEvent.Action.INC;

@SpringBootApplication
public class KafkaStreamsAggregateSample {


	public static void main(String[] args) {
		SpringApplication.run(KafkaStreamsAggregateSample.class, args);
	}


	final static String STORE_NAME = "balance-on-hand-counts";


	@EnableBinding(KafkaStreamsProcessorX.class)
	public class KafkaStreamsAggregateSampleApplication {

		@StreamListener("input")
		@SendTo("output")
		public KStream<String,SummaryEvent> process(KStream<String, DomainEvent> input) {

			ObjectMapper mapper = new ObjectMapper();
			Serde<SummaryEvent> summaryEventSerde = new JsonSerde<>(SummaryEvent.class, mapper);
			Serde<DomainEvent> domainEventSerde = new JsonSerde<>(DomainEvent.class, mapper);

			return input.groupByKey(Serialized.with(Serdes.String(), domainEventSerde))
					.aggregate(SummaryEvent::new,
							(key,  domainEvent,  summaryEvent) -> {
						int delta = domainEvent.getDelta();
						summaryEvent.setSource(domainEvent.getSource());
						if (domainEvent.getAction() == DEC) {
							summaryEvent.setCount(summaryEvent.getCount() - delta);
						} else if (domainEvent.getAction() == INC) {
							summaryEvent.setCount(summaryEvent.getCount() + delta);
						} else {
							return null;
						}
						return summaryEvent;
					}, Materialized.<String, SummaryEvent, KeyValueStore<Bytes, byte[]>>as(STORE_NAME)
							.withKeySerde(Serdes.String())
							.withValueSerde(summaryEventSerde))
					.toStream();


		}
	}

	interface KafkaStreamsProcessorX {
		@Input("input")
		KStream<?, ?> input();
		@Output("output")
		KStream<?, ?> output();
	}
}

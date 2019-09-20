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


			return input.join(
				input.mapValues(domainEvent -> {
					int delta;
					if (domainEvent.getAction().equals(DEC)) {
						delta = -domainEvent.getDelta();
					} else {
						delta = domainEvent.getDelta();
					}
					System.out.println(String.format("key: %s delta: %d", domainEvent.getKey(), delta));
					return delta;

				}).groupByKey(Serialized.with(Serdes.String(), Serdes.Integer()))
				  .aggregate(()-> new Integer(0),
								(key, i1,i2) -> {
									//System.out.println(String.format("key: %s i1: %d i2: %d",key,i1, i2));
									return i1 + i2;
								}, Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as(STORE_NAME)
								  .withKeySerde(Serdes.String())
								  .withValueSerde(Serdes.Integer())),
					(domainEvent, count) -> new SummaryEvent(domainEvent.getKey(),count,domainEvent.getSource())
				).groupByKey(Serialized.with(Serdes.String(), summaryEventSerde))
					.reduce((summaryEvent, v1) -> {

						String json = "";
						try {
							json = mapper.writeValueAsString(v1);
						} catch (Exception e) {

						}

						System.out.println(String.format("summary %s", json));
						return v1;
					}).toStream();


		}
	}

	interface KafkaStreamsProcessorX {
		@Input("input")
		KStream<?, ?> input();
		@Output("output")
		KStream<?, ?> output();
	}
}

package kafka.streams.table.join;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import static kafka.streams.table.join.DomainEvent.Action.DEC;

@RunWith(SpringRunner.class)
@SpringBootTest(
        properties = {
                "logging.level.org.apache=WARN"
        }
)
public class KafkaStreamsAggregateSampleTests {

    private KafkaTemplate<String, DomainEvent> kafkaTemplate;
    private Consumer<String, SummaryEvent> consumer;
    private Serde<DomainEvent> domainEventSerde;
    private Serde<SummaryEvent> summaryEventSerde;

    ObjectMapper mapper = new ObjectMapper();

    private static final String INPUT_TOPIC  =  "balance-on-hand";
    private static final String OUTPUT_TOPIC = "balance-on-hand-summary-event";
    private static final String GROUP_NAME   = "balance-on-hand-summary-test";

    final static String STORE_NAME = "balance-on-hand-counts";

    @ClassRule
    public static EmbeddedKafkaRule embeddedKafka = new EmbeddedKafkaRule(1, true, INPUT_TOPIC, OUTPUT_TOPIC);

    @BeforeClass
    public static void setup() {
        System.setProperty("spring.cloud.stream.kafka.streams.binder.brokers", embeddedKafka.getEmbeddedKafka().getBrokersAsString());
    }


    @Before
    public void setUp() {

        Map<String, Object> props = new HashMap<>();
        domainEventSerde = new JsonSerde<>(DomainEvent.class, mapper);
        summaryEventSerde = new JsonSerde<>(SummaryEvent.class, mapper);

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getEmbeddedKafka().getBrokersAsString());
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, domainEventSerde.serializer().getClass());

        DefaultKafkaProducerFactory<String, DomainEvent> pf = new DefaultKafkaProducerFactory<>(props);
        kafkaTemplate = new KafkaTemplate<>(pf, true);

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(GROUP_NAME, "false", embeddedKafka.getEmbeddedKafka());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put("key.deserializer", StringDeserializer.class);
        consumerProps.put("value.deserializer", summaryEventSerde.deserializer().getClass());
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "kafka.streams.table.join");
        DefaultKafkaConsumerFactory<String, SummaryEvent> cf = new DefaultKafkaConsumerFactory<>(consumerProps);

        consumer = cf.createConsumer();

    }

    @After
    public void tearDown() {
    }


    @Test
    public void processorInitialState() throws JsonProcessingException {
        sendMessages(1, 3);
        consumer.subscribe(Collections.singleton(OUTPUT_TOPIC));
        ConsumerRecords<String, SummaryEvent> records = consumer.poll(Duration.ofSeconds(1));
        consumer.commitSync();

        for (Iterator<ConsumerRecord<String, SummaryEvent>> it = records.iterator(); it.hasNext();) {
            ConsumerRecord<String, SummaryEvent> record = it.next();
            System.out.println(record.key() + ":" + mapper.writeValueAsString(record.value()));
        }
        //assertThat(records.count()).isEqualTo(100);
    }


    void sendMessages(int numberKeys, int eventsPerKey) {
        Random random = new Random();

        int[] balances = new int[numberKeys];

        for (int j = 0; j < numberKeys; j++) {
            for (int i = 0; i < eventsPerKey; i++) {
                DomainEvent ddEvent = new DomainEvent();
                ddEvent.setKey("key" + j);
                ddEvent.setSource("Source_"+ddEvent.getKey());
                ddEvent.setDelta(random.nextInt(10) + 1);
                ddEvent.setAction(random.nextBoolean() ? DEC : DomainEvent.Action.INC);
                if (ddEvent.getAction() == DEC) {
                    balances[j] -= ddEvent.getDelta();
                } else {
                    balances[j] += ddEvent.getDelta();
                }

                System.out.println(ddEvent.getKey() + " : " + ddEvent.getDelta() + " " + ddEvent.getAction());

                kafkaTemplate.send("balance-on-hand", ddEvent.getKey(), ddEvent);

            }
        }
        for (int i = 0; i < balances.length; i++) {
            System.out.println(String.format("%d : balance %d", i, balances[i]));
        }
    }

}

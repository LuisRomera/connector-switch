package org.luitro.connector;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.runner.RunWith;
import org.luitro.connector.config.Selector;
import org.luitro.connector.repository.RocksDBRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.stream.Collectors;


@EnableKafka
@RunWith(SpringRunner.class)
@DirtiesContext
@EmbeddedKafka(partitions = 1, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
public class EmbeddedKafkaIntegrationTest {

    @Autowired
    EmbeddedKafkaBroker kafkaBroker;

    @Autowired
    private RocksDBRepository repository;

    @Test
    public void sendRecordTest() {
        String bootstrapServers = "localhost:9092";
//
//        Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
//        KafkaProducer<Integer, String> producer = new KafkaProducer<>(senderProps);
//        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        Map<String, Object> producerProps = new HashMap<>(KafkaTestUtils.producerProps(kafkaBroker));
        Producer<String, String> producer = new DefaultKafkaProducerFactory<String, String>(new HashMap<>(producerProps)).createProducer();
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first_topic", "hello world");

        producer.send(producerRecord);
//
//        // flush data - synchronous
        producer.flush();

        String data = "Sending with our own simple KafkaProducer";

    }

//    private Consumer<Integer, String> configureConsumer() {
//        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testGroup", "true", embeddedKafkaBroker);
//        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        Consumer<Integer, String> consumer = new DefaultKafkaConsumerFactory<Integer, String>(consumerProps)
//                .createConsumer();
//        consumer.subscribe(Collections.singleton(TEST_TOPIC));
//        return consumer;
//    }

    @Test
    public void consumerTest() throws IOException {
//        new Selector(Files.lines(Paths.get("C:\\Users\\larom\\proyectos\\connector-switch\\src\\test\\resources\\elastic-kafka.json")).collect(Collectors.joining("\n")))
//                .switchConnector();

        repository.save("elastic-kafka", "key", "value");

        String value = repository.find("elastic-kafka", "key");

        System.out.println("as");
    }


}


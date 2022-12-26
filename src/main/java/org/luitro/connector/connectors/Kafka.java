package org.luitro.connector.connectors;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.luitro.connector.config.Constant.*;

public class Kafka {


    private final String topic;


    private final Properties properties;
    private KafkaConsumer<String, String> consumer;

    public Kafka(JsonObject kafkaJson, Type type) {

        topic = kafkaJson.get(TOPIC).getAsString();

        properties = new Properties();
        kafkaJson.keySet().stream()
                .filter(k -> !k.equals(TOPIC))
                .filter(k -> !k.equals(PARTITIONS))
                .forEach(k -> properties.put(k, kafkaJson.get(k).getAsString()));
        properties.put("enable.auto.commit", "false");
        properties.put("auto.offset.reset", "latest");


        switch (type){
            case CONSUMER:
                createConsumer(kafkaJson);
                break;
            case PRODUCER:
                createProducer(kafkaJson);
                break;
        }

    }

    public String getTopic() {
        return topic;
    }

    public KafkaConsumer<String, String> getConsumer() {
        return consumer;
    }

    private void createProducer(JsonObject kafkaJson) {

    }

    private void createConsumer(JsonObject kafkaJson) {

        consumer = new KafkaConsumer<>(properties);
        List<String> parts = new Gson()
                .fromJson(kafkaJson.get(PARTITIONS).getAsJsonArray(), new TypeToken<List<String>>() {
                }.getType());

        List<TopicPartition> topicPartitions = parts.stream()
                .map(i -> new TopicPartition(topic, Integer.parseInt(i)))
                .collect(Collectors.toList());

        consumer.assign(topicPartitions);
    }

    public enum Type {
        CONSUMER, PRODUCER
    }
}

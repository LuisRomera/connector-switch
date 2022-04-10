package org.luitro.connector.connectors;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.xcontent.XContentType;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import static org.luitro.connector.config.Constant.PARTITIONS;
import static org.luitro.connector.config.Constant.TOPIC;


public class KafkaElastic extends Thread {

    private final String topic;

    private final String index;

    private final KafkaConsumer<String, String> consumer;

    private RestHighLevelClient client;

    public KafkaElastic(JsonObject kafkaJson, JsonObject elasticJson) {

        topic = kafkaJson.get(TOPIC).getAsString();

        Properties consumerProps = new Properties();
        kafkaJson.keySet().stream()
                .filter(k -> !k.equals(TOPIC))
                .filter(k -> !k.equals(PARTITIONS))
                .forEach(k -> consumerProps.put(k, kafkaJson.get(k).getAsString()));
        consumerProps.put("enable.auto.commit", "false");
        consumerProps.put("auto.offset.reset", "latest");
        consumer = new KafkaConsumer<>(consumerProps);

        List<String> parts = new Gson().fromJson(kafkaJson.get(PARTITIONS).getAsJsonArray(), new TypeToken<List<String>>() {
        }.getType());

        List<TopicPartition> topicPartitions = parts.stream().map(i -> new TopicPartition(topic, Integer.parseInt(i)))
                .collect(Collectors.toList());
        consumer.assign(topicPartitions);

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(elasticJson.get("host").getAsString(),
                        elasticJson.get("port").getAsInt(), elasticJson.get("protocol").getAsString()));

        if (elasticJson.get("user") != null) {
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(elasticJson.get("user").getAsString(),
                            elasticJson.get("password").getAsString()));
            builder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                @Override
                public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                    return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                }
            });
        }
        client = new RestHighLevelClient(builder);
        index = elasticJson.get("index").getAsString();
    }

    @Override
    public void run() {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            for (ConsumerRecord<String, String> record : records) {
                try {
                    System.out.println(record.toString());
                    Map<TopicPartition, OffsetAndMetadata> currentOffsets =
                            new HashMap<TopicPartition, OffsetAndMetadata>();
                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1, "no metadata"));

                    BulkRequest request = new BulkRequest();
                    request.add(new IndexRequest(index).source(record.value(), XContentType.JSON));
                    BulkResponse bulkResponse = null;
                    bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
                    boolean result = Arrays.stream(bulkResponse.getItems()).anyMatch(BulkItemResponse::isFailed);
                    if (!result)
                        consumer.commitAsync(currentOffsets, null);

                } catch (Exception e) {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }
                    System.exit(0);
                }
            }
        }
    }
}

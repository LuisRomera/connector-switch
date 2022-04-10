package org.luitro.connector.config;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.luitro.connector.connectors.KafkaElastic;

import static org.luitro.connector.config.Constant.*;


public class Selector {

    private final String connector;

    private final JsonObject kafkaJson;

    private final JsonObject elasticJson;

    public Selector(String jsonString) {
        JsonObject json = JsonParser.parseString(jsonString).getAsJsonObject();
        this.connector = json.get(CONNECTOR).getAsString();
        this.kafkaJson = json.get(KAFKA).getAsJsonObject();
        this.elasticJson = json.getAsJsonObject(ELASTIC).getAsJsonObject();
    }

    public void switchConnector() {
        switch (connector) {
            case KAFKA_ELASTIC:
                Thread thread = new KafkaElastic(kafkaJson, elasticJson);
                thread.setName(connector);
                thread.start();
                break;
        }

    }
}

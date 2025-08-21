/*
 * Copyright 2025 Solace Corporation. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.solace.samples.rest.features.serdes.net.jsonschema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.solace.samples.rest.features.serdes.util.WaitForEnterThread;
import com.solace.samples.rest.features.serdes.util.Util;
import com.solace.serdes.Serializer;
import com.solace.serdes.common.SchemaHeaderId;
import com.solace.serdes.common.SerdeProperties;
import com.solace.serdes.common.resolver.config.SchemaResolverProperties;
import com.solace.serdes.jsonschema.JsonSchemaProperties;
import com.solace.serdes.jsonschema.JsonSchemaSerializer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * This class demonstrates how to use the Java 8+ HttpURLConnection with Json Schema serialization to produce messages over REST.
 * It serializes messages using the {@link JsonSchemaSerializer} and publishes it to a topic on a Solace message broker using an HTTP POST request.
 */
public class JsonSchemaRestPublisherHttpUrlConnection {
    private static final String REGISTRY_URL = Util.getEnv("REGISTRY_URL", "http://localhost:8081/apis/registry/v3");
    private static final String REGISTRY_USERNAME = Util.getEnv("REGISTRY_USERNAME", "sr-readonly");
    private static final String REGISTRY_PASSWORD = Util.getEnv("REGISTRY_PASSWORD", "roPassword");
    // BINARY Content-Type translates to Solace binary message and JSON Content-Type translates to Solace text message.
    // For more details on Solace Message Type Mapping to HTTP Content-Type, see:
    // https://docs.solace.com/API/RESTMessagingPrtl/Solace-REST-Message-Encoding.htm#solace-message-type-mapping-to-http-content-type
    private static final String CONTENT_TYPE = Util.getEnv("CONTENT_TYPE", "BINARY");
    private static final String TOPIC = Util.getEnv("TOPIC", "solace/samples/json");

    private final String brokerUrl;

    /**
     * Constructor for the publisher.
     * @param brokerUrl The URL of the broker.
     */
    public JsonSchemaRestPublisherHttpUrlConnection(String brokerUrl) {
        this.brokerUrl = brokerUrl;
    }

    /**
     * Main method to run the publisher.
     * @param args Command line arguments: <host> <port>
     * @throws IOException If an I/O error occurs.
     * @throws InterruptedException If the thread is interrupted.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 2) {
            System.out.printf("Usage: %s <host> <port> %n", JsonSchemaRestPublisherHttpUrlConnection.class.getName());
            System.exit(-1);
        }

        String host = args[0];
        String port = args[1];

        String brokerUrl = String.format("http://%s:%s/TOPIC/%s", host, port, TOPIC);
        JsonSchemaRestPublisherHttpUrlConnection publisher = new JsonSchemaRestPublisherHttpUrlConnection(brokerUrl);

        WaitForEnterThread exitListener = new WaitForEnterThread();
        exitListener.start();

        try (Serializer<JsonNode> serializer = new JsonSchemaSerializer<>()) {
            serializer.configure(getConfig());

            int index = 0;
            while (!exitListener.isDone()) {
                publisher.publish(serializer, String.format("UUID:%s", index));
                index++;
                Thread.sleep(100); // limit send rate
            }
        } catch (Exception e) {
            System.err.println("Error publishing message: " + e.getMessage());
            e.printStackTrace();
        }

        exitListener.join();
        System.out.println("Publisher shutdown.");
    }

    /**
     * Publishes a message.
     * @param serializer The serializer to use.
     * @param id The ID of the user.
     */
    public void publish(Serializer<JsonNode> serializer, String id) {
        ObjectNode user = new ObjectMapper().createObjectNode();
        HttpURLConnection connection = null;
        try {
            user.put("name", "John Doe");
            user.put("id", id);
            user.put("email", "support@solace.com");

            connection = serializeHttpMessage(brokerUrl, serializer, TOPIC, user);

            int responseCode = connection.getResponseCode();
            System.out.println("Published message with status: " + responseCode);
            System.out.println("Published message with record: " + user);

        } catch (Exception e) {
            System.err.println("Error publishing message: " + e.getMessage() + "\nfailed to publish object: " + user);
            e.printStackTrace();
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    /**
     * Serializes the HTTP message with the given payload and headers.
     * <p>
     * For more details on Solace-User-Property headers and data type mapping, see:
     * <ul>
     *   <li><a href="https://docs.solace.com/API/RESTMessagingPrtl/Solace-REST-Message-Encoding.htm#solace_message_custom_properties">Solace Message Custom Properties</a></li>
     *   <li><a href="https://docs.solace.com/API/RESTMessagingPrtl/Solace-REST-Message-Encoding.htm#solace-user-property-type">Solace User Property Type</a></li>
     * </ul>
     * @param urlStr The URL to connect to.
     * @param serializer The serializer to use.
     * @param topic The topic to publish to.
     * @param payload The payload to serialize.
     * @param <T> The type of the payload.
     * @return The HttpURLConnection object.
     * @throws IOException If an I/O error occurs.
     */
    public <T> HttpURLConnection serializeHttpMessage(String urlStr, Serializer<T> serializer, String topic, T payload) throws IOException {
        Map<String, Object> headers = new HashMap<>();
        byte[] payloadBytes = serializer.serialize(topic, payload, headers);

        URL url = new URL(urlStr);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("POST");
        connection.setDoOutput(true);

        for (String key : headers.keySet()) {
            Object value = headers.get(key);
            // No Integer/Long to String mapping is needed, as the SERDES schema ID header is already configured as a String.
            // The type parameter is optional. By default all Solace User Properties in a REST message are assumed to have a type "string"
            connection.setRequestProperty(String.format("%s%s", "Solace-User-Property-", key), value.toString());
        }

        if ("JSON".equals(CONTENT_TYPE)) {
            connection.setRequestProperty("Content-Type", "application/json");
        } else if ("BINARY".equals(CONTENT_TYPE)) {
            connection.setRequestProperty("Content-Type", "application/octet-stream");
        } else {
            connection.setRequestProperty("Content-Type", CONTENT_TYPE);
        }

        try (OutputStream os = connection.getOutputStream()) {
            os.write(payloadBytes);
            os.flush();
        }

        return connection;
    }

    /**
     * Gets the configuration for the serializer.
     * @return A map of configuration properties.
     */
    private static Map<String, Object> getConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(SchemaResolverProperties.REGISTRY_URL, REGISTRY_URL);
        config.put(SchemaResolverProperties.AUTH_USERNAME, REGISTRY_USERNAME);
        config.put(SchemaResolverProperties.AUTH_PASSWORD, REGISTRY_PASSWORD);
        // This configuration property will populate the SERDES header with a schema ID that is of type String
        config.put(SerdeProperties.SCHEMA_HEADER_IDENTIFIERS, SchemaHeaderId.SCHEMA_ID_STRING);
        return config;
    }
}
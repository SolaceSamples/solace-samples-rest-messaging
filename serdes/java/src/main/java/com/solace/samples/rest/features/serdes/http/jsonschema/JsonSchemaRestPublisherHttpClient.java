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

package com.solace.samples.rest.features.serdes.http.jsonschema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.solace.samples.rest.features.serdes.util.Util;
import com.solace.samples.rest.features.serdes.util.WaitForEnterThread;
import com.solace.serdes.Serializer;
import com.solace.serdes.common.SchemaHeaderId;
import com.solace.serdes.common.SerdeProperties;
import com.solace.serdes.common.resolver.config.SchemaResolverProperties;
import com.solace.serdes.jsonschema.JsonSchemaProperties;
import com.solace.serdes.jsonschema.JsonSchemaSerializer;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This class demonstrates how to use the Java 11+ HttpClient with Json Schema serialization to produce messages over REST.
 * It serializes messages using the {@link JsonSchemaSerializer} and publishes it to a topic on a Solace message broker using an HTTP POST request.
 */
public class JsonSchemaRestPublisherHttpClient implements AutoCloseable {
    private static final String REGISTRY_URL = Util.getEnv("REGISTRY_URL", "http://localhost:8081/apis/registry/v3");
    private static final String REGISTRY_USERNAME = Util.getEnv("REGISTRY_USERNAME", "sr-readonly");
    private static final String REGISTRY_PASSWORD = Util.getEnv("REGISTRY_PASSWORD", "roPassword");
    // BINARY Content-Type translates to Solace binary message and JSON Content-Type translates to Solace text message.
    // For more details on Solace Message Type Mapping to HTTP Content-Type, see:
    // https://docs.solace.com/API/RESTMessagingPrtl/Solace-REST-Message-Encoding.htm#solace-message-type-mapping-to-http-content-type
    private static final String CONTENT_TYPE = Util.getEnv("CONTENT_TYPE", "BINARY");
    private static final String TOPIC = Util.getEnv("TOPIC", "solace/samples/json");

    private final String brokerHost;
    private final int port;
    private final HttpClient client;
    private final ExecutorService executor;

    /**
     * Constructor for the publisher.
     * @param brokerHost The hostname or IP address of the broker.
     * @param port The port of the broker.
     */
    public JsonSchemaRestPublisherHttpClient(String brokerHost, int port) {
        this.brokerHost = brokerHost;
        this.port = port;
        this.executor = Executors.newCachedThreadPool();
        this.client = HttpClient.newBuilder()
                .executor(this.executor)
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    /**
     * Main method to run the publisher.
     * @param args Command line arguments: <host> <port>
     * @throws IOException If an I/O error occurs.
     * @throws InterruptedException If the thread is interrupted.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 2) {
            System.out.printf("Usage: %s <host> <port> %n", JsonSchemaRestPublisherHttpClient.class.getName());
            System.exit(-1);
        }

        String host = args[0];
        String port = args[1];

        int portNumber = Integer.parseInt(port);

        try (JsonSchemaRestPublisherHttpClient publisher = new JsonSchemaRestPublisherHttpClient(host, portNumber)) {
            WaitForEnterThread exitListener = new WaitForEnterThread();
            exitListener.start();

            try (Serializer<JsonNode> serializer = new JsonSchemaSerializer<>()) {
                serializer.configure(getConfig());

                int index = 0;
                while (!exitListener.isDone()) {
                    publisher.publishToTopic(serializer, TOPIC, String.format("UUID:%s", index));
                    index++;
                    Thread.sleep(100); // limit send rate
                }
            } catch (Exception e) {
                System.err.println("Error publishing message: " + e.getMessage());
                e.printStackTrace();
            }

            exitListener.join();
        }
        System.out.println("Publisher shutdown.");
    }

    @Override
    public void close() {
        executor.shutdown();
    }

    /**
     * Publishes a message to a specific topic.
     * @param serializer The serializer to use.
     * @param topic The topic to publish to.
     * @param id The ID of the user.
     */
    public void publishToTopic(Serializer<JsonNode> serializer, String topic, String id) {
        ObjectNode user = new ObjectMapper().createObjectNode();
        try {
            String url = String.format("http://%s:%d/TOPIC/%s", brokerHost, port, topic);
            HttpRequest.Builder httpBuilder = HttpRequest.newBuilder().uri(URI.create(url));

            user.put("name", "John Doe");
            user.put("id", id);
            user.put("email", "support@solace.com");

            HttpRequest request = serializeHttpMessage(httpBuilder, serializer, topic, user).build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            System.out.println("Published message with status: " + response.statusCode());
            System.out.println("Published message with record: " + user);

        } catch (Exception e) {
            System.err.println("Error publishing message: " + e.getMessage() + "\nfailed to publish object: " + user);
            e.printStackTrace();
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
     * @param httpBuilder The HTTP request builder.
     * @param serializer The serializer to use.
     * @param topic The topic to publish to.
     * @param payload The payload to serialize.
     * @param <T> The type of the payload.
     * @return The updated HTTP request builder.
     */
    public <T> HttpRequest.Builder serializeHttpMessage(HttpRequest.Builder httpBuilder, Serializer<T> serializer, String topic, T payload) {
        Map<String, Object> headers = new HashMap<>();
        byte[] payloadBytes = serializer.serialize(topic, payload, headers);

        for (String key : headers.keySet()) {
            Object value = headers.get(key);
            // No Integer/Long to String mapping is needed, as the SERDES schema ID header is already configured as a String.
            // The type parameter is optional. By default all Solace User Properties in a REST message are assumed to have a type "string"
            httpBuilder.header(String.format("%s%s", "Solace-User-Property-", key), value.toString());
        }

        if ("JSON".equals(CONTENT_TYPE)) {
            httpBuilder.header("Content-Type", "application/json");
        } else if ("BINARY".equals(CONTENT_TYPE)) {
            httpBuilder.header("Content-Type", "application/octet-stream");
        } else {
            httpBuilder.header("Content-Type", CONTENT_TYPE);
        }
        return httpBuilder.POST(HttpRequest.BodyPublishers.ofByteArray(payloadBytes));
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
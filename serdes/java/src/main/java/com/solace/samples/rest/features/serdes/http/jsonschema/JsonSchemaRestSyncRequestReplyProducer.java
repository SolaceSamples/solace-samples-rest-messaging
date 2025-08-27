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

import com.solace.samples.rest.features.serdes.util.Util;
import com.solace.samples.rest.features.serdes.util.WaitForEnterThread;
import com.solace.samples.serdes.jsonschema.CreateUser;
import com.solace.samples.serdes.jsonschema.CreateUserResponse;
import com.solace.serdes.Deserializer;
import com.solace.serdes.Serializer;
import com.solace.serdes.common.SchemaHeaderId;
import com.solace.serdes.common.SerdeProperties;
import com.solace.serdes.common.resolver.config.SchemaResolverProperties;
import com.solace.serdes.jsonschema.JsonSchemaDeserializer;
import com.solace.serdes.jsonschema.JsonSchemaSerializer;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class demonstrates how to use the Java 11+ HttpClient with Json Schema SERDES to send and receive synchronous request-reply messages over REST.
 * This sample performs the following steps:
 * <ol>
 *   <li>Configures a {@link JsonSchemaSerializer} to serialize a request object (e.g., {@code CreateUser}) into a JSON payload.</li>
 *   <li>Constructs an HTTP POST request targeting a specific topic on the broker.</li>
 *   <li>Uses the {@code Solace-Reply-Wait-Time-In-ms} header to instruct the broker to hold the HTTP connection open and wait for a reply from a consumer application.</li>
 *   <li>Sends the request and synchronously blocks, waiting for the HTTP response from the broker.</li>
 *   <li>Upon receiving the response, it extracts the reply payload from the HTTP response body.</li>
 *   <li>Uses a {@link JsonSchemaDeserializer} to deserialize the reply payload into a response object (e.g., {@code CreateUserResponse}).</li>
 * </ol>
 */
public class JsonSchemaRestSyncRequestReplyProducer implements AutoCloseable {

    private static final String REGISTRY_URL = Util.getEnv("REGISTRY_URL", "http://localhost:8081/apis/registry/v3");
    private static final String REGISTRY_USERNAME = Util.getEnv("REGISTRY_USERNAME", "sr-readonly");
    private static final String REGISTRY_PASSWORD = Util.getEnv("REGISTRY_PASSWORD", "roPassword");
    // BINARY Content-Type translates to Solace binary message and JSON Content-Type translates to Solace text message.
    // For more details on Solace Message Type Mapping to HTTP Content-Type, see:
    // https://docs.solace.com/API/RESTMessagingPrtl/Solace-REST-Message-Encoding.htm#solace-message-type-mapping-to-http-content-type
    private static final String CONTENT_TYPE = Util.getEnv("CONTENT_TYPE", "BINARY");
    private static final String REQUEST_TOPIC = Util.getEnv("REQUEST_TOPIC", "solace/samples/create-user/json");
    private static final String SOLACE_REPLY_WAIT_TIME_IN_MS = Util.getEnv("SOLACE_REPLY_WAIT_TIME_IN_MS", "10000");

    private final String brokerHost;
    private final int port;
    private final HttpClient client;
    private final ExecutorService executor;

    /**
     * Constructor for the REST synchronous request-reply producer.
     * @param brokerHost The hostname or IP address of the broker.
     * @param port The port of the broker.
     */
    public JsonSchemaRestSyncRequestReplyProducer(String brokerHost, int port) {
        this.brokerHost = brokerHost;
        this.port = port;
        this.executor = Executors.newCachedThreadPool();
        this.client = HttpClient.newBuilder()
                .executor(this.executor)
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    /**
     * Main method to run the REST synchronous request-reply producer.
     * @param args Command line arguments: <host> <port>
     * @throws IOException If an I/O error occurs.
     * @throws InterruptedException If the thread is interrupted.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 2) {
            System.out.printf("Usage: %s <host> <port> %n", JsonSchemaRestSyncRequestReplyProducer.class.getName());
            System.exit(-1);
        }

        String host = args[0];
        String port = args[1];

        int portNumber = Integer.parseInt(port);

        try (JsonSchemaRestSyncRequestReplyProducer producer = new JsonSchemaRestSyncRequestReplyProducer(host, portNumber)) {
            WaitForEnterThread exitListener = new WaitForEnterThread();
            exitListener.start();

            try (Serializer<CreateUser> serializer = new JsonSchemaSerializer<>();
                 Deserializer<CreateUserResponse> deserializer = new JsonSchemaDeserializer<>()) {
                serializer.configure(getSerializerConfig());
                deserializer.configure(getDeserializerConfig());

                while (!exitListener.isDone()) {
                    producer.publishRequestAndReceiveReply(serializer, deserializer, REQUEST_TOPIC);
                    Thread.sleep(100); // limit send rate
                }

            } catch (InterruptedException e) {
                System.err.println("Application loop was interrupted. Shutting down.");
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                System.err.println("A network error occurred: " + e.getMessage());
                e.printStackTrace();
            } catch (Exception e) {
                System.err.println("An unexpected error occurred: " + e.getMessage());
                e.printStackTrace();
            }
            exitListener.join();
        }
        System.out.println("Producer shutdown.");
    }

    @Override
    public void close() {
        executor.shutdown();
    }

    /**
     * Creates a user object, serializes it, sends it as a request, waits for a reply,
     * and deserializes the reply from the HTTP response.
     * @param serializer   The configured serializer for the request object.
     * @param deserializer The configured deserializer for the reply object.
     * @param requestTopic The topic to which the request message will be sent.
     */
    public void publishRequestAndReceiveReply(Serializer<CreateUser> serializer, Deserializer<CreateUserResponse> deserializer, String requestTopic) throws IOException, InterruptedException {
        CreateUser user = new CreateUser();
        user.setName("John Doe");
        user.setEmail("support@solace.com");

        try {
            String url = String.format("http://%s:%d/TOPIC/%s", brokerHost, port, requestTopic);
            HttpRequest.Builder httpBuilder = HttpRequest.newBuilder().uri(URI.create(url));

            HttpRequest request = serializeHttpMessage(httpBuilder, serializer, requestTopic, user).build();

            System.out.printf("%n- - - - - - - - - - SENDING SERDES REQUEST MESSAGE - - - - - - - - - -%n");
            printAllHeaders(request.headers());
            System.out.println("Body: " + user);
            System.out.printf("- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -%n");

            HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());

            if (response.body().length > 0) {
                Map<String, Object> headers = getSerdesHeaders(response.headers());
                // The destination is left empty since it is optional and not required.
                // However, the schema ID required for deserialization has been extracted
                // from the HTTP headers and passed into the deserialize call.
                CreateUserResponse responseUser = deserializer.deserialize("", response.body(), headers);
                System.out.printf("%n- - - - - - - - - - RECEIVED SERDES REPLY MESSAGE - - - - - - - - - -%n");
                System.out.println("Status: " + response.statusCode());
                printAllHeaders(response.headers());
                System.out.println("BodyBytesLength: " + response.body().length);
                System.out.println("Body: " + responseUser);
                System.out.printf("- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -%n");
            } else {
                System.out.println("Received reply with status: " + response.statusCode());
            }

        } catch (RuntimeException e) {
            System.err.println("Error publishing request message: " + e.getMessage() + "\nfailed to publish request object: " + user);
            e.printStackTrace();
        }
    }

    /**
     * Serializes the HTTP message with the given payload and headers.
     * <p>
     * For more details on Solace-Specific HTTP headers and data type mapping, see:
     * <ul>
     *   <li><a href="https://docs.solace.com/API/RESTMessagingPrtl/Solace-REST-Message-Encoding.htm#solace_message_custom_properties">Solace Message Custom Properties</a></li>
     *   <li><a href="https://docs.solace.com/API/RESTMessagingPrtl/Solace-REST-Message-Encoding.htm#solace-user-property-type">Solace User Property Type</a></li>
     *   <li><a href="https://docs.solace.com/API/RESTMessagingPrtl/Solace-REST-Message-Encoding.htm#solace-specific-http-headers">Solace Reply Wait Time In ms</a></li>
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
            // The type parameter is optional. By default, all Solace User Properties in a REST message are assumed to have a type "string"
            httpBuilder.header(String.format("%s%s", "Solace-User-Property-", key), value.toString());
        }

        // Specifies this is a request-reply message and sets the timeout for the broker to wait for a reply.
        httpBuilder.header("Solace-Reply-Wait-Time-In-ms", SOLACE_REPLY_WAIT_TIME_IN_MS);

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
     * Extracts the SERDES headers from the HTTP headers. For more details on Solace-User-Property headers, see:
     * <ul>
     *  <li><a href="https://docs.solace.com/API/RESTMessagingPrtl/Solace-REST-Message-Encoding.htm?Highlight=REST%20custom%20headers#solace-message-custom-properties">Solace Message Custom Properties</a></li>
     * </ul>
     * @param httpHeaders the HTTP headers
     * @return a map of SERDES headers
     */
    private static Map<String,Object> getSerdesHeaders(java.net.http.HttpHeaders httpHeaders) {
        final Map<String,Object> serdesHeaders = new HashMap<>(httpHeaders.map().size());
        final String SMF_USER_PROPERTY_PREFIX = "solace-user-property-";
        // define regex pattern to exact value and type from "<value> [; type=<type>]"
        final Pattern typePattern = Pattern.compile("(.*?)(?:\\s*;\\s*type=(\\S+))?$");
        httpHeaders.map().forEach((key, values) -> {
            // The com.sun.net.httpserver.Headers class normalizes its keys to adhere to the following format:
            // First character uppercase, all other characters lowercase.
            // To make extraction easier, convert the key to be lowercase.
            if (key.toLowerCase().startsWith(SMF_USER_PROPERTY_PREFIX)) {
                // get first http value
                String httpValue = values.get(0);

                // create matcher for http value parsing
                Matcher matcher = typePattern.matcher(httpValue);

                // find pattern matches
                if (!matcher.find()) {
                    // value pattern mismatch, skip value
                    return;
                }

                // extract value and type from http header value
                // value must be present
                String valueAsString = matcher.group(1).trim();
                // type can optionally be present, otherwise null
                String type = matcher.group(2);
                Object value = null;
                if (type != null) {
                    // The header has a type suffix, e.g., "123 ; type=int64".
                    // Valid types are:
                    // string, wchar, bool, int8, int16, int32, int64, uint8, uint16, uint32, uint64, float, double, null
                    if ("int64".equals(type.trim())) {
                        // convert http header value to type Long
                        // for SerdeHeader.SCHEMA_ID
                        // SCHEMA_ID must be Long for Solace deserializer
                        value = Long.parseLong(valueAsString);
                    }
                } else {
                    // No type suffix is present. This is used for string-based properties.
                    // This correctly handles SerdeHeaders.SCHEMA_ID_STRING, which is passed as a string.
                    value = valueAsString;
                }
                // Strip the "solace-user-property-" prefix to get the original message property key.
                serdesHeaders.put(key.substring(SMF_USER_PROPERTY_PREFIX.length()), value);
            }
        });

        return serdesHeaders;
    }

    /**
     * Prints all HTTP headers.
     * @param httpHeaders the HTTP headers from java.net.http
     */
    private static void printAllHeaders(HttpHeaders httpHeaders) {
        System.out.println("HttpHeaders:");
        httpHeaders.map().forEach((key, values) -> {
            System.out.printf("Key: [%s], Values: [%s]\n", key, String.join(", ", values));
        });
    }

    /**
     * Gets the configuration for the serializer.
     * @return A map of configuration properties.
     */
    private static Map<String, Object> getSerializerConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(SchemaResolverProperties.REGISTRY_URL, REGISTRY_URL);
        config.put(SchemaResolverProperties.AUTH_USERNAME, REGISTRY_USERNAME);
        config.put(SchemaResolverProperties.AUTH_PASSWORD, REGISTRY_PASSWORD);
        // This configuration property will populate the SERDES header with a schema ID that is of type String
        config.put(SerdeProperties.SCHEMA_HEADER_IDENTIFIERS, SchemaHeaderId.SCHEMA_ID_STRING);
        return config;
    }

    /**
     * Gets the configuration for the deserializer.
     * @return A map of configuration properties.
     */
    private static Map<String, Object> getDeserializerConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(SchemaResolverProperties.REGISTRY_URL, REGISTRY_URL);
        config.put(SchemaResolverProperties.AUTH_USERNAME, REGISTRY_USERNAME);
        config.put(SchemaResolverProperties.AUTH_PASSWORD, REGISTRY_PASSWORD);
        return config;
    }

}

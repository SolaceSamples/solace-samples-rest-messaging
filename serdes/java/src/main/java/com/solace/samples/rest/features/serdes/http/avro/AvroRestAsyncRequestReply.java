
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

package com.solace.samples.rest.features.serdes.http.avro;

import com.solace.samples.rest.features.serdes.util.Util;
import com.solace.samples.rest.features.serdes.util.WaitForEnterThread;
import com.solace.serdes.Deserializer;
import com.solace.serdes.SerializationException;
import com.solace.serdes.Serializer;
import com.solace.serdes.avro.AvroDeserializer;
import com.solace.serdes.avro.AvroProperties;
import com.solace.serdes.avro.AvroSerializer;
import com.solace.serdes.common.SchemaHeaderId;
import com.solace.serdes.common.SerdeHeaders;
import com.solace.serdes.common.SerdeProperties;
import com.solace.serdes.common.resolver.config.SchemaResolverProperties;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class demonstrates how to use the Java 11+ HttpClient with Avro SERDES to send and receive asynchronous request-reply messages over REST.
 * This sample performs the following steps:
 * <ol>
 *   <li>Starts an embedded HTTP server to listen for the asynchronous reply message.</li>
 *   <li>Configures a {@link AvroSerializer} to serialize a request record (e.g., {@code CreateUser}) into an Avro payload.</li>
 *   <li>Constructs an HTTP POST request targeting a specific topic on the broker.</li>
 *   <li>Uses the {@code Solace-Reply-To-Destination} header to specify where the consumer should send the reply.</li>
 *   <li>Sends the request and receives an immediate 200 OK from the broker.</li>
 *   <li>When the consumer application sends the reply, the embedded HTTP server receives it.</li>
 *   <li>Uses a {@link AvroDeserializer} to deserialize the reply payload into a response record (e.g., {@code CreateUserResponse}).</li>
 * </ol>
 * <p>
 * Note: The replying consumer for this sample is the JCSMP sample named AvroSerdesReplier.
 * This can be found in the public
 * <a href="https://github.com/SolaceSamples/solace-samples-java-jcsmp">
 * Solace Java JCSMP samples repository
 * </a>
 */
public class AvroRestAsyncRequestReply implements AutoCloseable {

    private static final String REGISTRY_URL = Util.getEnv("REGISTRY_URL", "http://localhost:8081/apis/registry/v3");
    private static final String REGISTRY_USERNAME = Util.getEnv("REGISTRY_USERNAME", "sr-readonly");
    private static final String REGISTRY_PASSWORD = Util.getEnv("REGISTRY_PASSWORD", "roPassword");
    // BINARY Content-Type translates to Solace binary message and JSON Content-Type translates to Solace text message.
    // For more details on Solace Message Type Mapping to HTTP Content-Type, see:
    // https://docs.solace.com/API/RESTMessagingPrtl/Solace-REST-Message-Encoding.htm#solace-message-type-mapping-to-http-content-type
    private static final String CONTENT_TYPE = Util.getEnv("CONTENT_TYPE", "BINARY");
    private static final String REQUEST_TOPIC = Util.getEnv("REQUEST_TOPIC", "solace/samples/create-user/avro");
    private static final String REPLY_TOPIC = Util.getEnv("REPLY_TOPIC", "solace/samples/create-user-response/avro");

    private final String brokerHost;
    private final int publishingPort;
    private final HttpClient client;
    private final ExecutorService executor;
    private final HttpServer replyServer;

    /**
     * Constructor for the REST asynchronous request-reply producer.
     * @param brokerHost The hostname or IP address where the broker is running.
     * @param publishingPort The publishing port of the broker.
     */
    public AvroRestAsyncRequestReply(String brokerHost, int publishingPort, int listenPort) throws IOException {
        this.brokerHost = brokerHost;
        this.publishingPort = publishingPort;
        this.executor = Executors.newCachedThreadPool();
        this.client = HttpClient.newBuilder()
                .executor(this.executor)
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        this.replyServer = HttpServer.create(new InetSocketAddress(listenPort), 0);
    }

    /**
     * Main method to run the REST asynchronous request-reply producer.
     * @param args Command line arguments: <host> <publishing-port> <reply-post-request-target> <listen-port> [<http-topic-header-key>]
     * @throws IOException If an I/O error occurs.
     * @throws InterruptedException If the thread is interrupted.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 4) {
            System.out.printf("Usage: %s <host> <publishing-port> <reply-post-request-target> <listen-port> [<http-topic-header-key>] %n", AvroRestAsyncRequestReply.class.getName());
            System.exit(-1);
        }

        String host = args[0];
        int portNumber = Integer.parseInt(args[1]);
        String replyPostRequestTarget = args[2];
        int listenPort = Integer.parseInt(args[3]);

        String httpTopicHeaderKey = "";
        if (args.length > 4) {
            httpTopicHeaderKey = args[4];
        }

        try (AvroRestAsyncRequestReply producer = new AvroRestAsyncRequestReply(host, portNumber, listenPort)) {
            WaitForEnterThread exitListener = new WaitForEnterThread();
            exitListener.start();
            try (Serializer<GenericRecord> serializer = new AvroSerializer<>();
                 Deserializer<GenericRecord> deserializer = new AvroDeserializer<>()) {
                serializer.configure(getSerializerConfig());
                deserializer.configure(getDeserializerConfig());
                producer.startReplyServer(deserializer, replyPostRequestTarget, httpTopicHeaderKey);

                while (!exitListener.isDone()) {
                    producer.publishRequest(serializer, REQUEST_TOPIC);
                    Thread.sleep(3000); // limit send rate
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

    public void startReplyServer(Deserializer<GenericRecord> deserializer, String replyPath, String httpTopicHeaderKey) {
        replyServer.createContext(replyPath, new SerdesMessageHandler(deserializer, Object::toString, httpTopicHeaderKey));
        replyServer.setExecutor(this.executor);
        replyServer.start();
        System.out.println("Reply server is running on port " + replyServer.getAddress().getPort());
    }

    @Override
    public void close() {
        executor.shutdown();
        replyServer.stop(0);
    }

    /**
     * Creates a user object, serializes it and sends it as a request.
     * and deserializes the reply from the HTTP response.
     * @param serializer   The configured serializer for the request object.
     * @param requestTopic The topic to which the request message will be sent.
     */
    public void publishRequest(Serializer<GenericRecord> serializer, String requestTopic) throws IOException, InterruptedException {
        GenericRecord user = initEmptyUserRecord();
        user.put("name", "John Doe");
        user.put("email", "support@solace.com");

        try {
            String url = String.format("http://%s:%d/TOPIC/%s", brokerHost, publishingPort, requestTopic);
            HttpRequest.Builder httpBuilder = HttpRequest.newBuilder().uri(URI.create(url));
            HttpRequest request = serializeHttpMessage(httpBuilder, serializer, requestTopic, user).build();

            System.out.printf("%n- - - - - - - - - - SENDING SERDES REQUEST MESSAGE - - - - - - - - - -%n");
            printAllHeaders(request.headers());
            System.out.println("Body: " + user);
            System.out.printf("- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -%n");

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            System.out.println("Received initial response with status: " + response.statusCode());
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
     *   <li><a href="https://docs.solace.com/API/RESTMessagingPrtl/Solace-REST-Message-Encoding.htm#solace-specific-http-headers">Solace Reply To Destination</a></li>
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
            httpBuilder.header(String.format("%s%s", "Solace-User-Property-", key), value.toString());
        }

        httpBuilder.header("Solace-Reply-To-Destination", String.format("/TOPIC/%s", REPLY_TOPIC));

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
     * Initializes an empty Avro GenericRecord based on the "create-user.avsc" schema for requests.
     *
     * @return An empty GenericRecord for the CreateUser schema
     * @throws IOException If there's an error reading the schema file
     */
    private static GenericRecord initEmptyUserRecord() throws IOException {
        try (InputStream rawSchema = AvroRestAsyncRequestReply.class.getResourceAsStream("/avro-schema/create-user.avsc")) {
            if (rawSchema == null) throw new IOException("Schema file not found");
            Schema schema = new SchemaParser().parse(rawSchema).mainSchema();
            return new GenericData.Record(schema);
        }
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
        if ("JSON".equals(CONTENT_TYPE)) {
            config.put(AvroProperties.ENCODING_TYPE, AvroProperties.AvroEncoding.JSON);
        } else {
            config.put(AvroProperties.ENCODING_TYPE, AvroProperties.AvroEncoding.BINARY);
        }
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

    /**
     * Inner class to handle messages received by the HTTP server.
     */
    static class SerdesMessageHandler implements HttpHandler {
        final String httpTopicHeaderKey;
        static final String SMF_USER_PROPERTY_PREFIX = "solace-user-property-";

        final Deserializer<GenericRecord> deserializer;
        final Function<GenericRecord, String> objectToStringFunc;

        /**
         * Constructor for the SerdesMessageHandler.
         * @param deserializer the deserializer to use
         * @param objectToString a function to convert the object to a string
         * @param httpTopicHeaderKey the HTTP header key for the topic
         */
        public SerdesMessageHandler(Deserializer<GenericRecord> deserializer, Function<GenericRecord, String> objectToString, String httpTopicHeaderKey) {
            this.deserializer = deserializer;
            this.objectToStringFunc = objectToString;
            this.httpTopicHeaderKey = httpTopicHeaderKey;
        }

        /**
         * Checks if the message is a SERDES message.
         * @param exchange the HTTP exchange
         * @return true if the message's http headers contain either SerdeHeaders.SCHEMA_ID and/or SerdeHeaders.SCHEMA_ID_STRING, false otherwise
         */
        public static boolean isSerdesMessage(HttpExchange exchange) {
            Headers httpHeaders = exchange.getRequestHeaders();
            // The SERDES Schema header for identification is SerdeHeaders.SCHEMA_ID and/or SerdeHeaders.SCHEMA_ID_STRING.
            return httpHeaders.containsKey(String.format("%s%s", SMF_USER_PROPERTY_PREFIX, SerdeHeaders.SCHEMA_ID))
                    || httpHeaders.containsKey(String.format("%s%s", SMF_USER_PROPERTY_PREFIX, SerdeHeaders.SCHEMA_ID_STRING));
        }

        /**
         * Extracts the SERDES headers from the HTTP headers. For more details on Solace-User-Property headers, see:
         * <ul>
         *  <li><a href="https://docs.solace.com/API/RESTMessagingPrtl/Solace-REST-Message-Encoding.htm?Highlight=REST%20custom%20headers#solace-message-custom-properties">Solace Message Custom Properties</a></li>
         * </ul>
         * @param httpHeaders the HTTP headers
         * @return a map of SERDES headers
         */
        private static Map<String,Object> getSerdesHeaders(Headers httpHeaders) {
            final Map<String,Object> serdesHeaders = new HashMap<>(httpHeaders.size());

            // define regex pattern to exact value and type from "<value> [; type=<type>]"
            final Pattern typePattern = Pattern.compile("(.*?)(?:\\s*;\\s*type=(\\S+))?$");
            httpHeaders.forEach((key, values) -> {
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
         * @param httpHeaders the HTTP headers
         */
        private static void printAllHeaders(Headers httpHeaders) {
            System.out.println("HttpHeaders:");
            httpHeaders.forEach((key, values) -> {
                System.out.printf("Key: [%s], Value: [%s]\n", key, values.get(0));
            });
        }

        /**
         * Handles a SERDES message.
         * @param exchange the HTTP exchange
         * @throws IOException if an I/O error occurs
         */
        public void handleSerdesMessage(HttpExchange exchange) throws IOException {
            Headers httpHeaders = exchange.getRequestHeaders();

            // If httpTopicHeaderKey is configured, use it to find the topic header.
            // Note: the topic is optional.
            Optional<String> requestTopic = Optional.of(httpTopicHeaderKey)
                    .filter(key -> !key.isEmpty())
                    .map(httpHeaders::getFirst);

            // Read the message body
            InputStream is = exchange.getRequestBody();
            byte[] messagePayload = is.readAllBytes();

            // extract SERDES headers from http headers
            Map<String, Object> serdesHeaders = getSerdesHeaders(httpHeaders);
            // deserialize with topic, payload and SERDES headers
            GenericRecord object = deserializer.deserialize(requestTopic.orElse(""), messagePayload, serdesHeaders);

            System.out.printf("%n- - - - - - - - - - RECEIVED ASYNC SERDES REPLY MESSAGE - - - - - - - - - -%n");
            printAllHeaders(httpHeaders);
            System.out.println("BodyBytesLength: " + messagePayload.length);
            System.out.println("Body: " + objectToStringFunc.apply(object));
            System.out.printf("- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -%n");

            // Acknowledge the receipt of the message
            sendResponse(exchange, 200, "".getBytes());
        }

        /**
         * Handles the HTTP request. For more details on configuring http status codes, see:
         * <ul>
         *  <li><a href="https://docs.solace.com/Services/Managing-RDPs.htm#configuring-http-status-codes">Configuring Http Status Codes</a></li>
         * </ul>
         * @param exchange the HTTP exchange
         * @throws IOException if an I/O error occurs
         */
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                if (!"POST".equals(exchange.getRequestMethod())) {
                    sendResponse(exchange, 405, "Must be a POST request".getBytes());
                    return;
                }

                if (isSerdesMessage(exchange)) {
                    handleSerdesMessage(exchange);
                } else {
                    printAllHeaders(exchange.getRequestHeaders());
                    sendResponse(exchange, 400, "Did not receive a SERDES message from POST request".getBytes());
                }
            } catch (SerializationException se) {
                se.printStackTrace();
                sendResponse(exchange, 400, "Failed to resolve schema".getBytes());
            } catch (Exception e) {
                e.printStackTrace();
                sendResponse(exchange, 500, "An internal server error occurred.".getBytes());
            }
        }

        /**
         * A helper method for sending an HTTP response.
         *
         * @param exchange The HttpExchange to respond to.
         * @param statusCode The HTTP status code.
         * @param responseBody The string to send as the response body. Can be empty.
         * @throws IOException
         */
        private void sendResponse(HttpExchange exchange, int statusCode, byte[] responseBody) throws IOException {
            exchange.sendResponseHeaders(statusCode, responseBody.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(responseBody);
            }
        }
    }
}

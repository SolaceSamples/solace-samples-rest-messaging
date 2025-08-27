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
import com.solace.samples.serdes.jsonschema.User;
import com.solace.serdes.Deserializer;
import com.solace.serdes.SerializationException;
import com.solace.serdes.common.SerdeHeaders;
import com.solace.serdes.common.resolver.config.SchemaResolverProperties;
import com.solace.serdes.jsonschema.JsonSchemaDeserializer;
import com.solace.serdes.jsonschema.JsonSchemaProperties;
import com.solace.serdes.jsonschema.JsonSchemaValidationException;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class demonstrates how to use the Java HttpServer with Json Schema deserialization to consume messages over REST.
 * It deserializes messages using the {@link JsonSchemaDeserializer} and consumes it from a topic on a Solace message broker using an HTTP POST request.
 */
public class JsonSchemaRestConsumer {

    public static final String REGISTRY_URL = Util.getEnv("REGISTRY_URL","http://localhost:8081/apis/registry/v3");
    public static final String REGISTRY_USERNAME =  Util.getEnv("REGISTRY_USERNAME", "sr-readonly");
    public static final String REGISTRY_PASSWORD =  Util.getEnv("REGISTRY_PASSWORD", "roPassword");

    /**
     * Main method to run the consumer.
     * @param args Command line arguments: <post-request-target> [<port>] [<http-topic-header-key>]
     * @throws IOException If an I/O error occurs.
     * @throws InterruptedException If the thread is interrupted.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 1) {
            System.out.println("Message delivery from a queue to a REST consumer cannot occur until a POST request target has been configured for the queue binding.");
            System.out.printf("Usage: %s <post-request-target> [<port>] [<http-topic-header-key>] %n", JsonSchemaRestConsumer.class.getName());
            System.exit(-1);
        }

        String postRequestTarget = args[0];

        int rdpPort = 8080; // default rest delivery port
        if (args.length > 1) {
            rdpPort = Integer.parseInt(args[1]);
        } else {
            System.out.println("To configure port pass in <port> as the second argument");
        }

        String httpTopicHeaderKey = "";
        if (args.length > 2) {
            httpTopicHeaderKey = args[2];
        }

        HttpServer server = HttpServer.create(new InetSocketAddress(rdpPort), 0);

        try (Deserializer<User> deserializer = new JsonSchemaDeserializer<>()) {
            deserializer.configure(getDeserializerConfig());

            // Create context for message endpoint
            server.createContext(postRequestTarget, new SerdesMessageHandler<User>(deserializer, Object::toString, httpTopicHeaderKey));

            // Start the server
            server.start();
            System.out.println("Server is running on port " + rdpPort);

            WaitForEnterThread exitListener = new WaitForEnterThread();
            exitListener.start();

            while (!exitListener.isDone()) {
                Thread.sleep(200);
            }
        }
    }

    /**
     * Returns a map of configuration properties for the deserializer.
     *
     * @return a map of configuration properties for the deserializer
     */
    private static Map<String, Object> getDeserializerConfig() {
        HashMap<String,Object> config = new HashMap<>();
        config.put(SchemaResolverProperties.REGISTRY_URL, REGISTRY_URL);
        config.put(SchemaResolverProperties.AUTH_USERNAME, REGISTRY_USERNAME);
        config.put(SchemaResolverProperties.AUTH_PASSWORD, REGISTRY_PASSWORD);
        config.put(JsonSchemaProperties.TYPE_PROPERTY, "customJavaType");
        return config;
    }

    /**
     * Inner class to handle messages received by the HTTP server.
     * @param <T> the type of the message payload
     */
    static class SerdesMessageHandler<T> implements HttpHandler {
        final String httpTopicHeaderKey;
        static final String SMF_USER_PROPERTY_PREFIX = "solace-user-property-";

        final Deserializer<T> deserializer;
        final Function<T, String> objectToStringFunc;

        /**
         * Constructor for the SerdesMessageHandler.
         * @param deserializer the deserializer to use
         * @param objectToString a function to convert the object to a string
         * @param httpTopicHeaderKey the HTTP header key for the topic
         */
        public SerdesMessageHandler(Deserializer<T> deserializer, Function<T, String> objectToString, String httpTopicHeaderKey) {
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
            Optional<String> optionalTopic = Optional.of(httpTopicHeaderKey)
                    .filter(key -> !key.isEmpty())
                    .map(httpHeaders::getFirst);

            // Read the message body
            InputStream is = exchange.getRequestBody();
            byte[] messagePayload = is.readAllBytes();

            // extract SERDES headers from http headers
            Map<String, Object> serdesHeaders = getSerdesHeaders(httpHeaders);
            // deserialize with topic, payload and SERDES headers
            T object = deserializer.deserialize(optionalTopic.orElse(""), messagePayload, serdesHeaders);

            System.out.printf("%n- - - - - - - - - - RECEIVED SERDES MESSAGE - - - - - - - - - -%n");
            printAllHeaders(httpHeaders);
            System.out.println("BodyBytesLength: " + messagePayload.length);
            System.out.println("Body: " + objectToStringFunc.apply(object));
            System.out.printf("- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -%n");
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
                    sendResponse(exchange, 405, "Must be a POST request");
                    return;
                }

                if (isSerdesMessage(exchange)) {
                    handleSerdesMessage(exchange);
                    sendResponse(exchange, 200, "Message received successfully");
                } else {
                    printAllHeaders(exchange.getRequestHeaders());
                    sendResponse(exchange, 400, "Did not receive a SERDES message from POST request");
                }
            } catch (JsonSchemaValidationException ve) {
                ve.printStackTrace();
                sendResponse(exchange, 422, "Failed to validate json schema");
            } catch (SerializationException se) {
                se.printStackTrace();
                sendResponse(exchange, 400, "Failed to resolve schema");
            } catch (Exception e) {
                e.printStackTrace();
                sendResponse(exchange, 500, "An internal server error occurred.");
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
        private void sendResponse(HttpExchange exchange, int statusCode, String responseBody) throws IOException {
            byte[] responseBytes = responseBody.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(statusCode, responseBytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(responseBytes);
            }
        }
    }
}

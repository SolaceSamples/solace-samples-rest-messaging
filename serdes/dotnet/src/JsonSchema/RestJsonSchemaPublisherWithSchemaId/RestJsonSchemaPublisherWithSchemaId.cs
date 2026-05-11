/*
 * Copyright 2026 Solace Corporation. All rights reserved.
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

using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Solace.SchemaRegistry.Serdes.JsonSchema;
using Resources.Serdes.JsonSchema;

namespace RestJsonSchemaPublisherWithSchemaId
{
    /// <summary>
    /// This class demonstrates how to use HttpClient with JSON Schema serialization to produce messages over REST.
    /// It serializes messages using the JsonSchemaSerializer and publishes them to a topic on a Solace message broker using HTTP POST requests.
    /// The schema ID is sent as a Long (int64) type in the Solace-User-Property header with a type suffix.
    /// </summary>
    class RestJsonSchemaPublisherWithSchemaId : IDisposable
    {
        private static readonly string RegistryUrl = Environment.GetEnvironmentVariable("REGISTRY_URL") ?? "http://localhost:8081/apis/registry/v3";
        private static readonly string RegistryUsername = Environment.GetEnvironmentVariable("REGISTRY_USERNAME") ?? "sr-readonly";
        private static readonly string RegistryPassword = Environment.GetEnvironmentVariable("REGISTRY_PASSWORD") ?? "roPassword";
        // BINARY Content-Type translates to Solace binary message and JSON Content-Type translates to Solace text message.
        // For more details on Solace Message Type Mapping to HTTP Content-Type, see:
        // https://docs.solace.com/API/RESTMessagingPrtl/Solace-REST-Message-Encoding.htm#solace-message-type-mapping-to-http-content-type
        private static readonly string ContentType = Environment.GetEnvironmentVariable("CONTENT_TYPE") ?? "BINARY";
        private static readonly string TopicName = Environment.GetEnvironmentVariable("TOPIC") ?? "solace/samples/json";

        private const string SolaceUserPropertyPrefix = "Solace-User-Property-";

        // Delay between publishing messages to improve readability of console output in this sample
        private const int PublishDelayMs = 100;

        // HTTP request timeout in seconds to allow for network delays and broker processing time
        private const int HttpTimeoutSeconds = 10;

        private readonly string _brokerHost;
        private readonly int _port;
        private readonly HttpClient _httpClient;

        // Flag to signal when to stop sending messages
        private static volatile bool _keepRunning = true;

        /// <summary>
        /// Constructor for the publisher.
        /// </summary>
        /// <param name="brokerHost">The hostname or IP address of the broker</param>
        /// <param name="port">The port of the broker</param>
        public RestJsonSchemaPublisherWithSchemaId(string brokerHost, int port)
        {
            _brokerHost = brokerHost;
            _port = port;
            _httpClient = new HttpClient
            {
                Timeout = TimeSpan.FromSeconds(HttpTimeoutSeconds)
            };
        }

        /// <summary>
        /// Main method to run the publisher.
        /// </summary>
        /// <param name="args">Command line arguments: &lt;host&gt; &lt;port&gt;</param>
        /// <returns>0 on success, 1 on failure</returns>
        static async Task<int> Main(string[] args)
        {
            if (args.Length < 2)
            {
                Console.WriteLine($"Usage: {typeof(RestJsonSchemaPublisherWithSchemaId).Name} <host> <port>");
                Console.WriteLine();
                Console.WriteLine("Schema Registry connection can be configured via environment variables:");
                Console.WriteLine("  REGISTRY_URL (default: http://localhost:8081/apis/registry/v3)");
                Console.WriteLine("  REGISTRY_USERNAME (default: sr-readonly)");
                Console.WriteLine("  REGISTRY_PASSWORD (default: roPassword)");
                Console.WriteLine("  CONTENT_TYPE (default: BINARY)");
                Console.WriteLine("  TOPIC (default: solace/samples/json)");
                return 1;
            }

            string host = args[0];
            if (!int.TryParse(args[1], out int portNumber))
            {
                Console.WriteLine($"Invalid port number: {args[1]}");
                return 1;
            }

            try
            {
                using (var publisher = new RestJsonSchemaPublisherWithSchemaId(host, portNumber))
                using (var serializer = new JsonSchemaSerializer<User>())
                {
                    // Configure the Schema Registry connection for the serializer
                    var config = GetSchemaRegistryConfig();
                    serializer.Configure(config);

                    // Start a thread to listen for Enter key press
                    Thread exitThread = new Thread(() =>
                    {
                        Console.WriteLine("Press Enter to exit.");
                        Console.ReadLine();
                        _keepRunning = false;
                    });
                    exitThread.Start();

                    // Publish messages continuously until user presses Enter
                    await publisher.PublishMessagesAsync(serializer);

                    // Wait for exit thread to complete
                    exitThread.Join();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception thrown: {ex.Message}");
                return 1;
            }

            Console.WriteLine("Publisher shutdown.");
            return 0;
        }

        /// <summary>
        /// Continuously publishes User messages to the topic until the user exits.
        /// </summary>
        /// <param name="serializer">The JSON Schema serializer to use for message serialization</param>
        private async Task PublishMessagesAsync(JsonSchemaSerializer<User> serializer)
        {
            int index = 0;

            while (_keepRunning)
            {
                try
                {
                    await PublishToTopicAsync(serializer, TopicName, $"UUID:{index}");
                    index++;

                    await Task.Delay(PublishDelayMs);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error in publish loop: {ex.Message}");
                }
            }

            Console.WriteLine("Stopped sending messages.");
        }

        /// <summary>
        /// Publishes a single message to a specific topic.
        /// </summary>
        /// <param name="serializer">The serializer to use</param>
        /// <param name="topic">The topic to publish to</param>
        /// <param name="id">The ID of the user</param>
        private async Task PublishToTopicAsync(JsonSchemaSerializer<User> serializer, string topic, string id)
        {
            // Create User object with sample data
            var user = new User
            {
                Name = "John Doe",
                Id = id,
                Email = "support@solace.com"
            };

            HttpRequestMessage request = null;
            HttpResponseMessage response = null;
            try
            {
                string url = $"http://{_brokerHost}:{_port}/TOPIC/{topic}";
                request = new HttpRequestMessage(HttpMethod.Post, url);

                // Serialize the message and get SERDES headers
                var headers = new Dictionary<string, object>();
                byte[] payload = await serializer.SerializeAsync(topic, user, headers);

                // Add SERDES headers as Solace-User-Property-* headers with type suffix (e.g., "123; type=int64")
                AddSerdesHeaders(request, headers);

                // Set content type and payload
                request.Content = new ByteArrayContent(payload);
                SetContentType(request, ContentType);

                // Send the HTTP request
                response = await _httpClient.SendAsync(request);
                if (response == null)
                {
                    Console.WriteLine("ERROR: Received null response from broker");
                    return;
                }

                Console.WriteLine($"Published message with status: {response.StatusCode}");
                Console.WriteLine($"Published message with record: {user}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error publishing message: {ex.Message}");
                Console.WriteLine($"Failed to publish object: {user}");
            }
            finally
            {
                request?.Dispose();
                response?.Dispose();
            }
        }

        /// <summary>
        /// Returns a configuration dictionary for the JSON Schema serializer.
        /// Contains the Schema Registry URL and authentication credentials.
        /// Uses default schema header identifier (Long type).
        /// </summary>
        /// <returns>A dictionary containing configuration properties</returns>
        private static Dictionary<string, object> GetSchemaRegistryConfig()
        {
            return new Dictionary<string, object>
            {
                { JsonSchemaPropertyKeys.RegistryUrl, RegistryUrl },
                { JsonSchemaPropertyKeys.AuthUsername, RegistryUsername },
                { JsonSchemaPropertyKeys.AuthPassword, RegistryPassword }
                // Default schema header identifier is Long (int64) type
            };
        }

        /// <summary>
        /// Disposes the HTTP client.
        /// </summary>
        public void Dispose()
        {
            _httpClient?.Dispose();
        }

        /// <summary>
        /// Adds SERDES headers to an HTTP request as Solace-User-Property-* headers.
        /// Header values include type suffixes based on the value type (int64, int32, or string).
        /// For more details on Solace-User-Property headers and data type mapping, see:
        /// https://docs.solace.com/API/RESTMessagingPrtl/Solace-REST-Message-Encoding.htm#solace_message_custom_properties
        /// https://docs.solace.com/API/RESTMessagingPrtl/Solace-REST-Message-Encoding.htm#solace-user-property-type
        /// </summary>
        /// <param name="request">The HTTP request message to add headers to</param>
        /// <param name="serdesHeaders">Dictionary of SERDES headers to add</param>
        private static void AddSerdesHeaders(HttpRequestMessage request, IDictionary<string, object> serdesHeaders)
        {
            if (request == null || serdesHeaders == null)
            {
                return;
            }
            // Headers must follow the format: Solace-User-Property-<serde header name>: <serde header value>; type=<type>
            // Valid types are: string, wchar, bool, int8, int16, int32, int64, uint8, uint16, uint32, uint64, float, double, null
            foreach (var kvp in serdesHeaders)
            {
                string headerName = $"{SolaceUserPropertyPrefix}{kvp.Key}";
                string headerValue;

                if (kvp.Value is long)
                {
                    // Add type suffix for Long values: "123; type=int64"
                    headerValue = $"{kvp.Value}; type=int64";
                }
                else if (kvp.Value is int)
                {
                    // Add type suffix for int values: "123; type=int32"
                    headerValue = $"{kvp.Value}; type=int32";
                }
                else
                {
                    // Default to string representation
                    headerValue = $"{kvp.Value?.ToString() ?? string.Empty}; type=string";
                }

                request.Headers.Add(headerName, headerValue);
            }
        }

        /// <summary>
        /// Sets the Content-Type header based on the content type string.
        /// BINARY translates to application/octet-stream (Solace binary message).
        /// JSON translates to application/json (Solace text message).
        /// For more details on Solace Message Type Mapping to HTTP Content-Type, see:
        /// https://docs.solace.com/API/RESTMessagingPrtl/Solace-REST-Message-Encoding.htm#solace-message-type-mapping-to-http-content-type
        /// </summary>
        /// <param name="request">The HTTP request message</param>
        /// <param name="contentType">The content type string (BINARY, JSON, or custom)</param>
        private static void SetContentType(HttpRequestMessage request, string contentType)
        {
            if (request == null || request.Content == null)
            {
                return;
            }

            if ("JSON".Equals(contentType, StringComparison.OrdinalIgnoreCase))
            {
                request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
            }
            else if ("BINARY".Equals(contentType, StringComparison.OrdinalIgnoreCase))
            {
                request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
            }
            else
            {
                request.Content.Headers.ContentType = new MediaTypeHeaderValue(contentType);
            }
        }
    }
}

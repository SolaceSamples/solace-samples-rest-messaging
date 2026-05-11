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
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Solace.SchemaRegistry.Serdes.Core;
//using Solace.SchemaRegistry.Serdes.Core.Resolver;
using Solace.SchemaRegistry.Serdes.JsonSchema;
using Resources.Serdes.JsonSchema;

namespace RestJsonSchemaSyncRequestReplyProducer
{
    /// <summary>
    /// This class demonstrates how to use HttpClient with JSON Schema SERDES to send and receive synchronous request-reply messages over REST.
    /// This sample performs the following steps:
    /// <list type="number">
    ///   <item>Configures a JsonSchemaSerializer to serialize a request object (e.g., CreateUser) into a JSON payload.</item>
    ///   <item>Constructs an HTTP POST request targeting a specific topic on the broker.</item>
    ///   <item>Uses the Solace-Reply-Wait-Time-In-ms header to instruct the broker to hold the HTTP connection open and wait for a reply from a consumer application.</item>
    ///   <item>Sends the request and synchronously blocks, waiting for the HTTP response from the broker.</item>
    ///   <item>Upon receiving the response, it extracts the reply payload from the HTTP response body.</item>
    ///   <item>Uses a JsonSchemaDeserializer to deserialize the reply payload into a response object (e.g., CreateUserResponse).</item>
    /// </list>
    /// </summary>
    class RestJsonSchemaSyncRequestReplyProducer : IDisposable
    {
        private static readonly string RegistryUrl = Environment.GetEnvironmentVariable("REGISTRY_URL") ?? "http://localhost:8081/apis/registry/v3";
        private static readonly string RegistryUsername = Environment.GetEnvironmentVariable("REGISTRY_USERNAME") ?? "sr-readonly";
        private static readonly string RegistryPassword = Environment.GetEnvironmentVariable("REGISTRY_PASSWORD") ?? "roPassword";
        // BINARY Content-Type translates to Solace binary message and JSON Content-Type translates to Solace text message.
        // For more details on Solace Message Type Mapping to HTTP Content-Type, see:
        // https://docs.solace.com/API/RESTMessagingPrtl/Solace-REST-Message-Encoding.htm#solace-message-type-mapping-to-http-content-type
        private static readonly string ContentType = Environment.GetEnvironmentVariable("CONTENT_TYPE") ?? "BINARY";
        private static readonly string RequestTopic = Environment.GetEnvironmentVariable("REQUEST_TOPIC") ?? "solace/samples/create-user/json";
        private static readonly string SolaceReplyWaitTimeInMs = Environment.GetEnvironmentVariable("SOLACE_REPLY_WAIT_TIME_IN_MS") ?? "10000";

        private const string SolaceUserPropertyPrefix = "Solace-User-Property-";
        private const string SolaceUserPropertyPrefixLower = "solace-user-property-";

        // Regex pattern to extract value and optional type from "<value> [; type=<type>]"
        private static readonly Regex TypePattern = new Regex(@"(.*?)(?:\s*;\s*type=(\S+))?$", RegexOptions.Compiled);

        private readonly string _brokerHost;
        private readonly int _port;
        private readonly HttpClient _httpClient;

        // Flag to signal when to stop sending messages
        private static volatile bool _keepRunning = true;

        /// <summary>
        /// Constructor for the REST synchronous request-reply producer.
        /// </summary>
        /// <param name="brokerHost">The hostname or IP address of the broker</param>
        /// <param name="port">The port of the broker</param>
        public RestJsonSchemaSyncRequestReplyProducer(string brokerHost, int port)
        {
            _brokerHost = brokerHost;
            _port = port;
            _httpClient = new HttpClient
            {
                Timeout = TimeSpan.FromSeconds(30) // Longer timeout for request-reply
            };
        }

        /// <summary>
        /// Main method to run the REST synchronous request-reply producer.
        /// </summary>
        /// <param name="args">Command line arguments: &lt;host&gt; &lt;port&gt;</param>
        /// <returns>0 on success, 1 on failure</returns>
        static async Task<int> Main(string[] args)
        {
            if (args.Length < 2)
            {
                Console.WriteLine($"Usage: {typeof(RestJsonSchemaSyncRequestReplyProducer).Name} <host> <port>");
                Console.WriteLine();
                Console.WriteLine("Schema Registry connection can be configured via environment variables:");
                Console.WriteLine("  REGISTRY_URL (default: http://localhost:8081/apis/registry/v3)");
                Console.WriteLine("  REGISTRY_USERNAME (default: sr-readonly)");
                Console.WriteLine("  REGISTRY_PASSWORD (default: roPassword)");
                Console.WriteLine("  CONTENT_TYPE (default: BINARY)");
                Console.WriteLine("  REQUEST_TOPIC (default: solace/samples/create-user/json)");
                Console.WriteLine("  SOLACE_REPLY_WAIT_TIME_IN_MS (default: 10000)");
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
                using (var producer = new RestJsonSchemaSyncRequestReplyProducer(host, portNumber))
                using (var serializer = new JsonSchemaSerializer<CreateUser>())
                using (var deserializer = new JsonSchemaDeserializer<CreateUserResponse>())
                {
                    // Configure serializer and deserializer
                    serializer.Configure(GetSerializerConfig());
                    deserializer.Configure(GetDeserializerConfig());

                    // Start a thread to listen for Enter key press
                    Thread exitThread = new Thread(() =>
                    {
                        Console.WriteLine("Press Enter to exit.");
                        Console.ReadLine();
                        _keepRunning = false;
                    });
                    exitThread.Start();

                    // Send request-reply messages continuously until user presses Enter
                    while (_keepRunning)
                    {
                        try
                        {
                            await producer.PublishRequestAndReceiveReplyAsync(serializer, deserializer, RequestTopic);
                            // Limit send rate
                            await Task.Delay(100);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Error in request-reply loop: {ex}");
                        }
                    }

                    exitThread.Join();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception thrown: {ex}");
                return 1;
            }

            Console.WriteLine("Producer shutdown.");
            return 0;
        }

        /// <summary>
        /// Creates a user object, serializes it, sends it as a request, waits for a reply,
        /// and deserializes the reply from the HTTP response.
        /// For more details on Solace-Specific HTTP headers, see:
        /// https://docs.solace.com/API/RESTMessagingPrtl/Solace-REST-Message-Encoding.htm#solace-specific-http-headers
        /// </summary>
        /// <param name="serializer">The configured serializer for the request object</param>
        /// <param name="deserializer">The configured deserializer for the reply object</param>
        /// <param name="requestTopic">The topic to which the request message will be sent</param>
        private async Task PublishRequestAndReceiveReplyAsync(
            JsonSchemaSerializer<CreateUser> serializer,
            JsonSchemaDeserializer<CreateUserResponse> deserializer,
            string requestTopic)
        {
            // Create request object
            var createUser = new CreateUser
            {
                Name = "John Doe",
                Email = "support@solace.com"
            };

            try
            {
                string url = $"http://{_brokerHost}:{_port}/TOPIC/{requestTopic}";
                var request = new HttpRequestMessage(HttpMethod.Post, url);

                // Serialize the request
                var requestHeaders = new Dictionary<string, object>();
                byte[] requestPayload = await serializer.SerializeAsync(requestTopic, createUser, requestHeaders);

                // Add SERDES headers
                AddSerdesHeaders(request, requestHeaders);

                // Add Solace-Reply-Wait-Time-In-ms header to enable synchronous request-reply
                request.Headers.Add("Solace-Reply-Wait-Time-In-ms", SolaceReplyWaitTimeInMs);

                // Set content type and payload
                request.Content = new ByteArrayContent(requestPayload);
                SetContentType(request, ContentType);

                Console.WriteLine();
                Console.WriteLine("- - - - - - - - - - SENDING SERDES REQUEST MESSAGE - - - - - - - - - -");
                PrintAllHeaders(request.Headers);
                Console.WriteLine($"Body: {createUser}");
                Console.WriteLine("- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -");

                // Send the request and wait for response
                var response = await _httpClient.SendAsync(request);

                if (response.IsSuccessStatusCode)
                {
                    byte[] replyPayload = await response.Content.ReadAsByteArrayAsync();
                    if (replyPayload.Length == 0)
                    {
                        Console.WriteLine($"Received success reply with status: {response.StatusCode} but body was empty.");
                        return;
                    }

                    // Extract SERDES headers from response
                    var replyHeaders = ExtractSerdesHeaders(response.Headers);

                    // Deserialize the reply (destination is left empty since it's optional)
                    CreateUserResponse reply = await deserializer.DeserializeAsync("", replyPayload, replyHeaders);

                    Console.WriteLine();
                    Console.WriteLine("- - - - - - - - - - RECEIVED SERDES REPLY MESSAGE - - - - - - - - - -");
                    Console.WriteLine($"Status: {response.StatusCode}");
                    PrintAllHeaders(response.Headers);
                    Console.WriteLine($"BodyBytesLength: {replyPayload.Length}");
                    Console.WriteLine($"Body: {reply}");
                    Console.WriteLine("- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -");
                }
                else
                {
                    string errorBody = await response.Content.ReadAsStringAsync();
                    Console.WriteLine($"Received error reply with status: {response.StatusCode}, body: {errorBody}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error publishing request message: {ex}");
                Console.WriteLine($"Failed to publish request object: {createUser}");
            }
        }

        /// <summary>
        /// Prints all HTTP headers to the console.
        /// </summary>
        /// <param name="headers">The HTTP headers</param>
        private static void PrintAllHeaders(System.Net.Http.Headers.HttpHeaders headers)
        {
            Console.WriteLine("HttpHeaders:");
            foreach (var header in headers)
            {
                Console.WriteLine($"Key: [{header.Key}], Values: [{string.Join(", ", header.Value)}]");
            }
        }

        /// <summary>
        /// Returns a configuration dictionary for the JSON Schema serializer.
        /// Contains the Schema Registry URL and authentication credentials.
        /// Configures the schema header identifier as a String type.
        /// </summary>
        /// <returns>A dictionary containing configuration properties</returns>
        private static Dictionary<string, object> GetSerializerConfig()
        {
            return new Dictionary<string, object>
            {
                { JsonSchemaPropertyKeys.RegistryUrl, RegistryUrl },
                { JsonSchemaPropertyKeys.AuthUsername, RegistryUsername },
                { JsonSchemaPropertyKeys.AuthPassword, RegistryPassword },
                // This configuration property will populate the SERDES header with a schema ID that is of type String
                { JsonSchemaPropertyKeys.SchemaHeaderIdentifiers, SchemaHeaderId.SchemaIdString }
            };
        }

        /// <summary>
        /// Returns a configuration dictionary for the JSON Schema deserializer.
        /// Contains the Schema Registry URL and authentication credentials.
        /// </summary>
        /// <returns>A dictionary containing configuration properties</returns>
        private static Dictionary<string, object> GetDeserializerConfig()
        {
            return new Dictionary<string, object>
            {
                { JsonSchemaPropertyKeys.RegistryUrl, RegistryUrl },
                { JsonSchemaPropertyKeys.AuthUsername, RegistryUsername },
                { JsonSchemaPropertyKeys.AuthPassword, RegistryPassword }
            };
        }

        /// <summary>
        /// Adds SERDES headers to an HTTP request as Solace-User-Property-* headers.
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

            foreach (var kvp in serdesHeaders)
            {
                string headerName = $"{SolaceUserPropertyPrefix}{kvp.Key}";
                string headerValue = kvp.Value?.ToString() ?? string.Empty;
                request.Headers.Add(headerName, headerValue);
            }
        }

        /// <summary>
        /// Extracts SERDES headers from HTTP headers.
        /// Parses headers in the format "solace-user-property-*" and extracts value and optional type.
        /// For more details on Solace-User-Property headers, see:
        /// https://docs.solace.com/API/RESTMessagingPrtl/Solace-REST-Message-Encoding.htm#solace-message-custom-properties
        /// </summary>
        /// <param name="httpHeaders">The HTTP headers to extract from</param>
        /// <returns>A dictionary of SERDES headers with parsed values</returns>
        private static Dictionary<string, object> ExtractSerdesHeaders(HttpHeaders httpHeaders)
        {
            var serdesHeaders = new Dictionary<string, object>();

            if (httpHeaders == null)
            {
                return serdesHeaders;
            }

            foreach (var header in httpHeaders)
            {
                // Check if header starts with solace-user-property- (case-insensitive)
                if (header.Key.StartsWith(SolaceUserPropertyPrefixLower, StringComparison.OrdinalIgnoreCase))
                {
                    // Get first value from the header
                    string httpValue = header.Value.FirstOrDefault();
                    if (string.IsNullOrEmpty(httpValue))
                    {
                        continue;
                    }

                    // Parse value and optional type using regex
                    Match matcher = TypePattern.Match(httpValue);
                    if (!matcher.Success)
                    {
                        // Pattern mismatch, skip this header
                        continue;
                    }

                    // Extract value (required) and type (optional)
                    string valueAsString = matcher.Groups[1].Value.Trim();
                    string type = matcher.Groups[2].Success ? matcher.Groups[2].Value : null;

                    object value;
                    if (type != null && "int64".Equals(type.Trim(), StringComparison.OrdinalIgnoreCase))
                    {
                        // Convert to Long for int64 type
                        // This is required for SerdeHeaders.SCHEMA_ID
                        if (long.TryParse(valueAsString, out long longValue))
                        {
                            value = longValue;
                        }
                        else
                        {
                            // Fallback to string if parsing fails — log so the malformed header is diagnosable
                            Console.WriteLine($"Warning: Header '{header.Key}' with type=int64 could not be parsed as long (value: '{valueAsString}'). Falling back to string, schema ID lookup may fail.");
                            value = valueAsString;
                        }
                    }
                    else
                    {
                        // No type suffix or other types - use string
                        // This correctly handles SerdeHeaders.SCHEMA_ID_STRING
                        value = valueAsString;
                    }

                    // Strip the "solace-user-property-" prefix to get the original message property key
                    // Normalize to lowercase to ensure case-insensitive matching with SerdeHeaders constants
                    string key = header.Key.Substring(SolaceUserPropertyPrefixLower.Length).ToLowerInvariant();
                    serdesHeaders[key] = value;
                }
            }

            return serdesHeaders;
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

        /// <summary>
        /// Disposes the HTTP client.
        /// </summary>
        public void Dispose()
        {
            _httpClient?.Dispose();
        }
    }
}

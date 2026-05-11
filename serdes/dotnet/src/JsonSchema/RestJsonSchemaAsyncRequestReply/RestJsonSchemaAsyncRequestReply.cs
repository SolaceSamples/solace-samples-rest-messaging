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
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Solace.SchemaRegistry.Serdes.Core;
using Solace.SchemaRegistry.Serdes.JsonSchema;
using Resources.Serdes.JsonSchema;
using Solace.Serdes;


namespace RestJsonSchemaAsyncRequestReply
{
    /// <summary>
    /// This class demonstrates how to use HttpClient with JSON Schema SERDES to send and receive asynchronous request-reply messages over REST.
    /// This sample performs the following steps:
    /// <list type="number">
    ///   <item>Starts an embedded HTTP server to listen for the asynchronous reply message.</item>
    ///   <item>Configures a JsonSchemaSerializer to serialize a request object (e.g., CreateUser) into a JSON payload.</item>
    ///   <item>Constructs an HTTP POST request targeting a specific topic on the broker.</item>
    ///   <item>Uses the Solace-Reply-To-Destination header to specify where the consumer should send the reply.</item>
    ///   <item>Sends the request and receives an immediate 200 OK from the broker.</item>
    ///   <item>When the consumer application sends the reply, the embedded HTTP server receives it.</item>
    ///   <item>Uses a JsonSchemaDeserializer to deserialize the reply payload into a response object (e.g., CreateUserResponse).</item>
    /// </list>
    /// </summary>
    class RestJsonSchemaAsyncRequestReply : IDisposable
    {
        private static readonly string RegistryUrl = Environment.GetEnvironmentVariable("REGISTRY_URL") ?? "http://localhost:8081/apis/registry/v3";
        private static readonly string RegistryUsername = Environment.GetEnvironmentVariable("REGISTRY_USERNAME") ?? "sr-readonly";
        private static readonly string RegistryPassword = Environment.GetEnvironmentVariable("REGISTRY_PASSWORD") ?? "roPassword";
        // BINARY Content-Type translates to Solace binary message and JSON Content-Type translates to Solace text message.
        // For more details on Solace Message Type Mapping to HTTP Content-Type, see:
        // https://docs.solace.com/API/RESTMessagingPrtl/Solace-REST-Message-Encoding.htm#solace-message-type-mapping-to-http-content-type
        private static readonly string ContentType = Environment.GetEnvironmentVariable("CONTENT_TYPE") ?? "BINARY";
        private static readonly string RequestTopic = Environment.GetEnvironmentVariable("REQUEST_TOPIC") ?? "solace/samples/create-user/json";
        private static readonly string ReplyTopic = Environment.GetEnvironmentVariable("REPLY_TOPIC") ?? "solace/samples/create-user-response/json";
        private const string SolaceUserPropertyPrefix = "Solace-User-Property-";

        private readonly string _brokerHost;
        private readonly int _publishingPort;
        private readonly HttpClient _httpClient;
        private readonly HttpListener _replyListener;

        // Flag to signal when to stop
        private static volatile bool _keepRunning = true;

        /// <summary>
        /// Constructor for the REST asynchronous request-reply producer.
        /// </summary>
        /// <param name="brokerHost">The hostname or IP address where the broker is running</param>
        /// <param name="publishingPort">The publishing port of the broker</param>
        /// <param name="listenPort">The port for the embedded reply server</param>
        /// <param name="replyPostRequestTarget">The POST request target for replies</param>
        public RestJsonSchemaAsyncRequestReply(string brokerHost, int publishingPort, int listenPort, string replyPostRequestTarget)
        {
            _brokerHost = brokerHost;
            _publishingPort = publishingPort;
            _httpClient = new HttpClient
            {
                Timeout = TimeSpan.FromSeconds(10)
            };

            // Create and configure the reply listener
            _replyListener = new HttpListener();
            _replyListener.Prefixes.Add($"http://+:{listenPort}{replyPostRequestTarget}/");
        }

        /// <summary>
        /// Main method to run the REST asynchronous request-reply producer.
        /// </summary>
        /// <param name="args">Command line arguments: &lt;host&gt; &lt;publishing-port&gt; &lt;reply-post-request-target&gt; &lt;listen-port&gt; [&lt;http-topic-header-key&gt;]</param>
        /// <returns>0 on success, 1 on failure</returns>
        static async Task<int> Main(string[] args)
        {
            if (args.Length < 4)
            {
                Console.WriteLine($"Usage: {typeof(RestJsonSchemaAsyncRequestReply).Name} <host> <publishing-port> <reply-post-request-target> <listen-port> [<http-topic-header-key>]");
                Console.WriteLine();
                Console.WriteLine("Schema Registry connection can be configured via environment variables:");
                Console.WriteLine("  REGISTRY_URL (default: http://localhost:8081/apis/registry/v3)");
                Console.WriteLine("  REGISTRY_USERNAME (default: sr-readonly)");
                Console.WriteLine("  REGISTRY_PASSWORD (default: roPassword)");
                Console.WriteLine("  CONTENT_TYPE (default: BINARY)");
                Console.WriteLine("  REQUEST_TOPIC (default: solace/samples/create-user/json)");
                Console.WriteLine("  REPLY_TOPIC (default: solace/samples/create-user-response/json)");
                return 1;
            }

            string host = args[0];
            if (!int.TryParse(args[1], out int publishingPort))
            {
                Console.WriteLine($"Invalid publishing port number: {args[1]}");
                return 1;
            }

            string replyPostRequestTarget = args[2];

            if (!int.TryParse(args[3], out int listenPort))
            {
                Console.WriteLine($"Invalid listen port number: {args[3]}");
                return 1;
            }

            string httpTopicHeaderKey = "";
            if (args.Length > 4)
            {
                httpTopicHeaderKey = args[4];
            }

            try
            {
                using (var producer = new RestJsonSchemaAsyncRequestReply(host, publishingPort, listenPort, replyPostRequestTarget))
                using (var serializer = new JsonSchemaSerializer<CreateUser>())
                using (var deserializer = new JsonSchemaDeserializer<CreateUserResponse>())
                {
                    // Configure serializer and deserializer
                    serializer.Configure(GetSerializerConfig());
                    deserializer.Configure(GetDeserializerConfig());

                    // Start the reply server
                    producer.StartReplyServer(deserializer, httpTopicHeaderKey);

                    // Start a thread to listen for Enter key press
                    Thread exitThread = new Thread(() =>
                    {
                        Console.WriteLine("Press Enter to exit.");
                        Console.ReadLine();
                        _keepRunning = false;
                    });
                    exitThread.Start();

                    // Send request messages continuously
                    while (_keepRunning)
                    {
                        try
                        {
                            await producer.PublishRequestAsync(serializer, RequestTopic);

                            // Limit send rate
                            await Task.Delay(3000);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Error in request loop: {ex}");
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
        /// Starts the embedded HTTP server to receive asynchronous reply messages.
        /// </summary>
        /// <param name="deserializer">The deserializer for reply messages</param>
        /// <param name="httpTopicHeaderKey">Optional HTTP header key containing the topic</param>
        public void StartReplyServer(JsonSchemaDeserializer<CreateUserResponse> deserializer, string httpTopicHeaderKey)
        {
            _replyListener.Start();
            Console.WriteLine("Reply server is running");

            // Start async task to handle incoming replies
            _ = Task.Run(async () =>
            {
                while (_keepRunning)
                {
                    try
                    {
                        var context = await _replyListener.GetContextAsync();
                        _ = HandleReplyAsync(context, deserializer, httpTopicHeaderKey);
                    }
                    catch (HttpListenerException)
                    {
                        // Listener was stopped
                        break;
                    }
                    catch (ObjectDisposedException)
                    {
                        // Listener was closed (e.g., during Dispose race)
                        break;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error receiving reply: {ex}");
                    }
                }
            });
        }

        /// <summary>
        /// Creates a user object, serializes it, and sends it as a request.
        /// For more details on Solace-Specific HTTP headers, see:
        /// https://docs.solace.com/API/RESTMessagingPrtl/Solace-REST-Message-Encoding.htm#solace-specific-http-headers
        /// </summary>
        /// <param name="serializer">The configured serializer for the request object</param>
        /// <param name="requestTopic">The topic to which the request message will be sent</param>
        private async Task PublishRequestAsync(JsonSchemaSerializer<CreateUser> serializer, string requestTopic)
        {
            // Create request object
            var createUser = new CreateUser
            {
                Name = "John Doe",
                Email = "support@solace.com"
            };

            try
            {
                string url = $"http://{_brokerHost}:{_publishingPort}/TOPIC/{requestTopic}";
                var request = new HttpRequestMessage(HttpMethod.Post, url);

                // Serialize the request
                var requestHeaders = new Dictionary<string, object>();
                byte[] requestPayload = await serializer.SerializeAsync(requestTopic, createUser, requestHeaders);

                // Add SERDES headers (without type suffix)
                AddSerdesHeaders(request, requestHeaders);

                // Add Solace-Reply-To-Destination header for asynchronous request-reply
                request.Headers.Add("Solace-Reply-To-Destination", $"/TOPIC/{ReplyTopic}");

                // Set content type and payload
                request.Content = new ByteArrayContent(requestPayload);
                SetContentType(request, ContentType);

                Console.WriteLine();
                Console.WriteLine("- - - - - - - - - - SENDING SERDES REQUEST MESSAGE - - - - - - - - - -");
                PrintAllHeaders(request.Headers);
                Console.WriteLine($"Body: {createUser}");
                Console.WriteLine("- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -");

                // Send the request (will receive immediate 200 OK from broker)
                var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();
                Console.WriteLine($"Received initial response with status: {response.StatusCode}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error publishing request message: {ex}");
                Console.WriteLine($"Failed to publish request object: {createUser}");
            }
        }

        /// <summary>
        /// Checks if the message is a SERDES message.
        /// </summary>
        /// <param name="context">The HTTP listener context</param>
        /// <returns>true if the message's HTTP headers contain either schema_id and/or schema_id_string, false otherwise</returns>
        private static bool IsSerdesMessage(HttpListenerContext context)
        {
            var httpHeaders = context.Request.Headers;
            // The SERDES Schema header for identification is schema_id and/or schema_id_string.
            return httpHeaders.AllKeys.Any(key => key != null &&
                (key.Equals($"{SolaceUserPropertyPrefix}{SerdeHeaders.SchemaId}", StringComparison.OrdinalIgnoreCase) ||
                 key.Equals($"{SolaceUserPropertyPrefix}{SerdeHeaders.SchemaIdString}", StringComparison.OrdinalIgnoreCase)));
        }

        /// <summary>
        /// Handles an asynchronous reply message received by the embedded HTTP server.
        /// </summary>
        /// <param name="context">The HTTP listener context</param>
        /// <param name="deserializer">The JSON Schema deserializer for replies</param>
        /// <param name="httpTopicHeaderKey">Optional HTTP header key containing the topic</param>
        private static async Task HandleReplyAsync(
            HttpListenerContext context,
            JsonSchemaDeserializer<CreateUserResponse> deserializer,
            string httpTopicHeaderKey)
        {
            try
            {
                // Extract topic if header key is configured
                string replyTopic = "";
                if (!string.IsNullOrEmpty(httpTopicHeaderKey))
                {
                    replyTopic = context.Request.Headers[httpTopicHeaderKey] ?? "";
                }

                // Read the reply body
                byte[] replyPayload;
                using (var ms = new MemoryStream())
                {
                    await context.Request.InputStream.CopyToAsync(ms);
                    replyPayload = ms.ToArray();
                }

                Dictionary<string, object> replyHeaders;
                if (IsSerdesMessage(context))
                {
                    // Extract SERDES headers from HTTP response headers
                    replyHeaders = ExtractSerdesHeaders(context.Request.Headers);
                }
                else
                {
                    PrintAllHeaders(context.Request.Headers);
                    Console.WriteLine($"Error did not receive a SERDES message from POST request.");

                    // Send error response
                    try
                    {
                        context.Response.StatusCode = 400;
                        context.Response.ContentLength64 = 0;
                        context.Response.Close();
                    }
                    catch (Exception responseEx)
                    {
                        Console.WriteLine($"Failed to send error response: {responseEx.GetType().Name}: {responseEx.Message}");
                    }
                    return;
                }

                // Debug: Print extracted SERDES headers
                foreach (var kvp in replyHeaders)
                {
                    Console.WriteLine($"  {kvp.Key} = {kvp.Value} (Type: {kvp.Value?.GetType().Name})");
                }

                if (replyHeaders.Count == 0)
                {
                    Console.WriteLine($"WARNING: Received {replyPayload.Length}-byte payload with no SERDES headers. " +
                                      "The replier may not be attaching schema ID headers.");
                    context.Response.StatusCode = 200;
                    context.Response.ContentLength64 = 0;
                    context.Response.Close();
                    return;
                }

                // Deserialize the reply
                CreateUserResponse reply = await deserializer.DeserializeAsync(replyTopic, replyPayload, replyHeaders);

                Console.WriteLine();
                Console.WriteLine("- - - - - - - - - - RECEIVED SERDES REPLY MESSAGE - - - - - - - - - -");
                PrintAllHeaders(context.Request.Headers);
                Console.WriteLine($"BodyBytesLength: {replyPayload.Length}");
                Console.WriteLine($"Body: {reply}");
                Console.WriteLine("- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -");

                // Send 200 OK response
                context.Response.StatusCode = 200;
                context.Response.ContentLength64 = 0;
                context.Response.Close();
            }
            catch (JsonSchemaValidationException ex)
            {
                Console.WriteLine($"Error handling reply: {ex}");

                // Send error response
                try
                {
                    context.Response.StatusCode = 422;
                    context.Response.ContentLength64 = 0;
                    context.Response.Close();
                }
                catch (Exception responseEx)
                {
                    Console.WriteLine($"Failed to send error response: {responseEx.GetType().Name}: {responseEx.Message}");
                }
            }
            catch (SerializationException ex)
            {
                Console.WriteLine($"Error handling reply: {ex}");

                // Send error response
                try
                {
                    context.Response.StatusCode = 400;
                    context.Response.ContentLength64 = 0;
                    context.Response.Close();
                }
                catch (Exception responseEx)
                {
                    Console.WriteLine($"Failed to send error response: {responseEx.GetType().Name}: {responseEx.Message}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error handling reply: {ex}");

                // Send error response
                try
                {
                    context.Response.StatusCode = 500;
                    context.Response.ContentLength64 = 0;
                    context.Response.Close();
                }
                catch (Exception responseEx)
                {
                    Console.WriteLine($"Failed to send error response: {responseEx.GetType().Name}: {responseEx.Message}");
                }
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
        /// Prints all HTTP headers to the console (NameValueCollection).
        /// </summary>
        /// <param name="headers">The name-value collection of headers</param>
        private static void PrintAllHeaders(System.Collections.Specialized.NameValueCollection headers)
        {
            Console.WriteLine("HttpHeaders:");
            foreach (string key in headers.AllKeys)
            {
                if (key != null)
                {
                    Console.WriteLine($"Key: [{key}], Values: [{headers[key]}]");
                }
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
        /// Disposes resources.
        /// </summary>
        public void Dispose()
        {
            _httpClient?.Dispose();
            if (_replyListener?.IsListening == true)
            {
                try
                {
                    _replyListener.Stop();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error stopping reply listener: {ex.GetType().Name}: {ex.Message}");
                }
            }
            try
            {
                _replyListener?.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error closing reply listener: {ex.GetType().Name}: {ex.Message}");
            }
        }

        // Constants for Solace REST messaging headers
        private const string SolaceUserPropertyPrefixLower = "solace-user-property-";

        // Regex pattern to extract value and optional type from "<value> [; type=<type>]"
        private static readonly System.Text.RegularExpressions.Regex TypePattern = new System.Text.RegularExpressions.Regex(@"(.*?)(?:\s*;\s*type=(\S+))?$", System.Text.RegularExpressions.RegexOptions.Compiled);

        /// <summary>
        /// Adds SERDES headers to an HTTP request as Solace-User-Property-* headers.
        /// Header values are added as raw strings without type suffixes.
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
        /// Extracts SERDES headers from NameValueCollection (used with HttpListener).
        /// Parses headers in the format "solace-user-property-*" and extracts value and optional type.
        /// </summary>
        /// <param name="headers">The name-value collection of headers</param>
        /// <returns>A dictionary of SERDES headers with parsed values</returns>
        private static Dictionary<string, object> ExtractSerdesHeaders(System.Collections.Specialized.NameValueCollection headers)
        {
            var serdesHeaders = new Dictionary<string, object>();

            if (headers == null)
            {
                return serdesHeaders;
            }

            foreach (string key in headers.AllKeys)
            {
                if (key == null)
                {
                    continue;
                }

                // Check if header starts with solace-user-property- (case-insensitive)
                if (key.StartsWith(SolaceUserPropertyPrefixLower, StringComparison.OrdinalIgnoreCase))
                {
                    string httpValue = headers[key];
                    if (string.IsNullOrEmpty(httpValue))
                    {
                        continue;
                    }

                    // Parse value and optional type using regex
                    System.Text.RegularExpressions.Match matcher = TypePattern.Match(httpValue);
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
                        if (long.TryParse(valueAsString, out long longValue))
                        {
                            value = longValue;
                        }
                        else
                        {
                            Console.WriteLine($"WARNING: Header '{key}' declared type=int64 but '{valueAsString}' " +
                                              "could not be parsed as long. Falling back to string, schema ID lookup may fail.");
                            value = valueAsString;
                        }
                    }
                    else
                    {
                        // No type suffix or other types - use string
                        value = valueAsString;
                    }

                    // Strip the "solace-user-property-" prefix to get the original message property key
                    // Normalize to lowercase to ensure case-insensitive matching with SerdeHeaders constants
                    string propertyKey = key.Substring(SolaceUserPropertyPrefixLower.Length).ToLowerInvariant();
                    serdesHeaders[propertyKey] = value;
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
                request.Content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/json");
            }
            else if ("BINARY".Equals(contentType, StringComparison.OrdinalIgnoreCase))
            {
                request.Content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/octet-stream");
            }
            else
            {
                request.Content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue(contentType);
            }
        }
    }
}

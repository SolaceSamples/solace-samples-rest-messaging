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
using System.Collections.Specialized;
using System.IO;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Solace.SchemaRegistry.Serdes.Core;
//using Solace.SchemaRegistry.Serdes.Core.Resolver;
using Solace.SchemaRegistry.Serdes.JsonSchema;
using Solace.Serdes;
using Resources.Serdes.JsonSchema;

namespace RestJsonSchemaSyncRequestReplyConsumer
{
    /// <summary>
    /// This class demonstrates how to use HttpListener with JSON Schema SERDES to receive and send synchronous request-reply messages over REST.
    /// This sample performs the following steps:
    /// <list type="number">
    ///   <item>Configures a JsonSchemaDeserializer to deserialize an incoming request payload from an HTTP request body into a request object (e.g., CreateUser).</item>
    ///   <item>Listens for an HTTP POST request from the Solace event broker, which is triggered by a producer sending a request message.</item>
    ///   <item>After processing the deserialized request, it creates a reply object (e.g., CreateUserResponse).</item>
    ///   <item>Uses a JsonSchemaSerializer to serialize the reply object.</item>
    ///   <item>Sends the serialized reply payload back to the broker by writing it into the HTTP response body with a 200 OK status.</item>
    ///   <item>The broker then delivers this HTTP response payload back to the original requesting client, completing the exchange.</item>
    /// </list>
    /// </summary>
    class RestJsonSchemaSyncRequestReplyConsumer : IDisposable
    {
        private static readonly string RegistryUrl = Environment.GetEnvironmentVariable("REGISTRY_URL") ?? "http://localhost:8081/apis/registry/v3";
        private static readonly string RegistryUsername = Environment.GetEnvironmentVariable("REGISTRY_USERNAME") ?? "sr-readonly";
        private static readonly string RegistryPassword = Environment.GetEnvironmentVariable("REGISTRY_PASSWORD") ?? "roPassword";
        private static readonly string ReplyTopic = Environment.GetEnvironmentVariable("REPLY_TOPIC") ?? "solace/samples/create-user-response/json";

        private const string SolaceUserPropertyPrefixLower = "solace-user-property-";

        // Regex pattern to extract value and optional type from "<value> [; type=<type>]"
        private static readonly Regex TypePattern = new Regex(@"(.*?)(?:\s*;\s*type=(\S+))?$", RegexOptions.Compiled);

        private readonly HttpListener _listener;

        private static volatile bool _keepRunning = true;

        /// <summary>
        /// Constructor for the REST synchronous request-reply consumer.
        /// </summary>
        /// <param name="port">The port for the HTTP listener</param>
        /// <param name="postRequestTarget">The POST request target path</param>
        public RestJsonSchemaSyncRequestReplyConsumer(int port, string postRequestTarget)
        {
            _listener = new HttpListener();
            _listener.Prefixes.Add($"http://+:{port}{postRequestTarget}");
        }

        /// <summary>
        /// Main method to run the REST synchronous request-reply consumer.
        /// </summary>
        /// <param name="args">Command line arguments: &lt;post-request-target&gt; [&lt;port&gt;] [&lt;http-topic-header-key&gt;]</param>
        /// <returns>0 on success, 1 on failure</returns>
        static async Task<int> Main(string[] args)
        {
            if (args.Length < 1)
            {
                Console.WriteLine("Message delivery from a queue to a REST consumer cannot occur until a POST request target has been configured for the queue binding.");
                Console.WriteLine($"Usage: {typeof(RestJsonSchemaSyncRequestReplyConsumer).Name} <post-request-target> [<port>] [<http-topic-header-key>]");
                return 1;
            }

            string postRequestTarget = args[0].TrimEnd('/') + "/";

            int port = 8080; // default rest delivery port
            if (args.Length > 1)
            {
                if (!int.TryParse(args[1], out port))
                {
                    Console.WriteLine($"Invalid port number: {args[1]}");
                    return 1;
                }
            }
            else
            {
                Console.WriteLine("To configure port pass in <port> as the second argument");
            }

            string httpTopicHeaderKey = "";
            if (args.Length > 2)
            {
                httpTopicHeaderKey = args[2];
            }

            try
            {
                using (var consumer = new RestJsonSchemaSyncRequestReplyConsumer(port, postRequestTarget))
                using (var deserializer = new JsonSchemaDeserializer<CreateUser>())
                using (var serializer = new JsonSchemaSerializer<CreateUserResponse>())
                {
                    deserializer.Configure(GetDeserializerConfig());
                    serializer.Configure(GetSerializerConfig());

                    // Start the HTTP listener
                    consumer._listener.Start();

                    Console.WriteLine($"Server is running on port {port}");
                    Console.WriteLine("Press Enter to exit.");

                    // Start a thread to listen for Enter key press
                    Thread exitThread = new Thread(() =>
                    {
                        Console.ReadLine();
                        _keepRunning = false;
                        consumer._listener.Stop();
                    });
                    exitThread.Start();

                    // Process incoming requests
                    while (_keepRunning)
                    {
                        try
                        {
                            // Wait for an incoming request
                            var context = await consumer._listener.GetContextAsync();

                            // Handle the request
                            await HandleRequestAsync(context, deserializer, serializer, httpTopicHeaderKey);
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
                            Console.WriteLine($"Error accepting request: {ex}");
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

            Console.WriteLine("Server shutdown.");
            return 0;
        }

        /// <summary>
        /// Handles an incoming HTTP request.
        /// For more details on configuring HTTP status codes, see:
        /// https://docs.solace.com/Services/Managing-RDPs.htm#configuring-http-status-codes
        /// </summary>
        /// <param name="context">The HTTP listener context</param>
        /// <param name="deserializer">The JSON Schema deserializer for requests</param>
        /// <param name="serializer">The JSON Schema serializer for replies</param>
        /// <param name="httpTopicHeaderKey">Optional HTTP header key containing the topic</param>
        private static async Task HandleRequestAsync(
            HttpListenerContext context,
            JsonSchemaDeserializer<CreateUser> deserializer,
            JsonSchemaSerializer<CreateUserResponse> serializer,
            string httpTopicHeaderKey)
        {
            try
            {
                // Validate POST request
                if (!"POST".Equals(context.Request.HttpMethod, StringComparison.OrdinalIgnoreCase))
                {
                    await SendResponseAsync(context, 405, Encoding.UTF8.GetBytes("Must be a POST request"));
                    return;
                }

                // Check if this is a SERDES message
                if (!IsSerdesMessage(context))
                {
                    PrintAllHeaders(context.Request.Headers);
                    await SendResponseAsync(context, 400, Encoding.UTF8.GetBytes("Did not receive a SERDES message from POST request"));
                    return;
                }

                // Handle the SERDES message
                await HandleSerdesMessageAsync(context, deserializer, serializer, httpTopicHeaderKey);
            }
            catch (JsonSchemaValidationException ve)
            {
                Console.WriteLine(ve);
                try
                {
                    await SendResponseAsync(context, 422, Encoding.UTF8.GetBytes("Failed to validate json schema"));
                }
                catch (Exception responseEx)
                {
                    Console.WriteLine($"Failed to send error response: {responseEx.GetType().Name}: {responseEx.Message}");
                }
            }
            catch (SerializationException se)
            {
                Console.WriteLine(se);
                try
                {
                    await SendResponseAsync(context, 400, Encoding.UTF8.GetBytes("Failed to resolve schema"));
                }
                catch (Exception responseEx)
                {
                    Console.WriteLine($"Failed to send error response: {responseEx.GetType().Name}: {responseEx.Message}");
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                try
                {
                    await SendResponseAsync(context, 500, Encoding.UTF8.GetBytes("An internal server error occurred."));
                }
                catch (Exception responseEx)
                {
                    Console.WriteLine($"Failed to send error response: {responseEx.GetType().Name}: {responseEx.Message}");
                }
            }
        }

        /// <summary>
        /// Handles a SERDES request-reply message by deserializing the request, creating a reply, and serializing the response.
        /// </summary>
        /// <param name="context">The HTTP listener context</param>
        /// <param name="deserializer">The JSON Schema deserializer for requests</param>
        /// <param name="serializer">The JSON Schema serializer for replies</param>
        /// <param name="httpTopicHeaderKey">Optional HTTP header key containing the topic</param>
        private static async Task HandleSerdesMessageAsync(
            HttpListenerContext context,
            JsonSchemaDeserializer<CreateUser> deserializer,
            JsonSchemaSerializer<CreateUserResponse> serializer,
            string httpTopicHeaderKey)
        {
            // Extract topic if header key is configured
            string requestTopic = "";
            if (!string.IsNullOrEmpty(httpTopicHeaderKey))
            {
                requestTopic = context.Request.Headers[httpTopicHeaderKey] ?? "";
            }

            // Read the request body
            byte[] requestPayload;
            using (var ms = new MemoryStream())
            {
                await context.Request.InputStream.CopyToAsync(ms);
                requestPayload = ms.ToArray();
            }

            // Extract SERDES headers from HTTP request headers
            var requestHeaders = ExtractSerdesHeaders(context.Request.Headers);

            // Deserialize the request
            CreateUser createUser = await deserializer.DeserializeAsync(requestTopic, requestPayload, requestHeaders);

            Console.WriteLine();
            Console.WriteLine("- - - - - - - - - - RECEIVED SERDES REQUEST MESSAGE - - - - - - - - - -");
            PrintAllHeaders(context.Request.Headers);
            Console.WriteLine($"BodyBytesLength: {requestPayload.Length}");
            Console.WriteLine($"Body: {createUser}");
            Console.WriteLine("- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -");

            // Create a reply with a generated ID
            var userResponse = new CreateUserResponse
            {
                Id = Guid.NewGuid().ToString().Substring(0, 8)
            };

            // Serialize the reply
            var replyHeaders = new Dictionary<string, object>();
            byte[] replyPayload = await serializer.SerializeAsync(ReplyTopic, userResponse, replyHeaders);

            // Add SERDES headers to HTTP response headers
            // NOTE: This assumes serializer is configured with SchemaHeaderId.SchemaIdString.
            // If using the default numeric SchemaId, long values must be encoded with "; type=int64" suffix.
            foreach (var kvp in replyHeaders)
            {
                context.Response.Headers.Add($"Solace-User-Property-{kvp.Key}", kvp.Value?.ToString() ?? string.Empty);
            }

            Console.WriteLine();
            Console.WriteLine("- - - - - - - - - - SENDING SERDES REPLY MESSAGE - - - - - - - - - -");
            PrintAllHeaders(context.Response.Headers);
            Console.WriteLine($"BodyBytesLength: {replyPayload.Length}");
            Console.WriteLine($"Body: {userResponse}");
            Console.WriteLine("- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -");

            // Send the reply
            await SendResponseAsync(context, 200, replyPayload);
        }

        /// <summary>
        /// Checks if the request contains SERDES schema identification headers.
        /// </summary>
        /// <param name="context">The HTTP listener context</param>
        /// <returns>True if the message's HTTP headers contain either SerdeHeaders.SchemaId and/or SerdeHeaders.SchemaIdString</returns>
        private static bool IsSerdesMessage(HttpListenerContext context)
        {
            // The SERDES Schema header for identification is SerdeHeaders.SchemaId and/or SerdeHeaders.SchemaIdString.
            string schemaIdHeader = $"{SolaceUserPropertyPrefixLower}{SerdeHeaders.SchemaId}";
            string schemaIdStringHeader = $"{SolaceUserPropertyPrefixLower}{SerdeHeaders.SchemaIdString}";

            foreach (string key in context.Request.Headers.AllKeys)
            {
                if (key != null &&
                    (key.Equals(schemaIdHeader, StringComparison.OrdinalIgnoreCase) ||
                     key.Equals(schemaIdStringHeader, StringComparison.OrdinalIgnoreCase)))
                {
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Sends an HTTP response.
        /// </summary>
        /// <param name="context">The HTTP listener context</param>
        /// <param name="statusCode">The HTTP status code</param>
        /// <param name="responseBody">The response body bytes</param>
        private static async Task SendResponseAsync(HttpListenerContext context, int statusCode, byte[] responseBody)
        {
            context.Response.StatusCode = statusCode;
            context.Response.ContentLength64 = responseBody.Length;

            if (responseBody.Length > 0)
            {
                await context.Response.OutputStream.WriteAsync(responseBody, 0, responseBody.Length);
            }

            context.Response.Close();
        }

        /// <summary>
        /// Prints all HTTP headers to the console.
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
        /// Extracts SERDES headers from NameValueCollection (used with HttpListener).
        /// Parses headers in the format "solace-user-property-*" and extracts value and optional type.
        /// </summary>
        /// <param name="headers">The name-value collection of headers</param>
        /// <returns>A dictionary of SERDES headers with parsed values</returns>
        private static Dictionary<string, object> ExtractSerdesHeaders(NameValueCollection headers)
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
                        if (long.TryParse(valueAsString, out long longValue))
                        {
                            value = longValue;
                        }
                        else
                        {
                            // Fallback to string if parsing fails — log so the malformed header is diagnosable
                            Console.WriteLine($"Warning: Header with type=int64 could not be parsed as long (value: '{valueAsString}'). Treating as string.");
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
        /// Disposes resources.
        /// </summary>
        public void Dispose()
        {
            if (_listener?.IsListening == true)
            {
                try
                {
                    _listener.Stop();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error stopping listener: {ex.GetType().Name}: {ex.Message}");
                }
            }
            try
            {
                _listener?.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error closing listener: {ex.GetType().Name}: {ex.Message}");
            }
        }
    }
}

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
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Solace.SchemaRegistry.Serdes.JsonSchema;
using Resources.Serdes.JsonSchema;

namespace RestJsonSchemaConsumer
{
    /// <summary>
    /// This class demonstrates how to use HttpListener with JSON Schema SERDES to receive messages over REST.
    /// It listens for HTTP POST requests from a Solace message broker and deserializes the message payload
    /// using the JsonSchemaDeserializer.
    /// For more details on configuring HTTP status codes, see:
    /// https://docs.solace.com/Services/Managing-RDPs.htm#configuring-http-status-codes
    /// </summary>
    class RestJsonSchemaConsumer
    {
        private static readonly string RegistryUrl = Environment.GetEnvironmentVariable("REGISTRY_URL") ?? "http://localhost:8081/apis/registry/v3";
        private static readonly string RegistryUsername = Environment.GetEnvironmentVariable("REGISTRY_USERNAME") ?? "sr-readonly";
        private static readonly string RegistryPassword = Environment.GetEnvironmentVariable("REGISTRY_PASSWORD") ?? "roPassword";
        // Regex pattern to extract value and optional type from "<value> [; type=<type>]"
        private static readonly Regex typePattern = new Regex(@"(.*?)(?:\s*;\s*type=(\S+))?$", RegexOptions.Compiled);

        private static volatile bool _keepRunning = true;

        /// <summary>
        /// Main method to run the REST consumer.
        /// </summary>
        /// <param name="args">Command line arguments: &lt;post-request-target&gt; [&lt;port&gt;] [&lt;http-topic-header-key&gt;]</param>
        /// <returns>0 on success, 1 on failure</returns>
        static async Task<int> Main(string[] args)
        {
            if (args.Length < 1)
            {
                Console.WriteLine("Message delivery from a queue to a REST consumer cannot occur until a POST request target has been configured for the queue binding.");
                Console.WriteLine($"Usage: {typeof(RestJsonSchemaConsumer).Name} <post-request-target> [<port>] [<http-topic-header-key>]");
                return 1;
            }

            string postRequestTarget = args[0];

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
                // Create and configure the deserializer
                using (var deserializer = new JsonSchemaDeserializer<User>())
                using (var listener = new HttpListener())
                {
                    var config = GetDeserializerConfig();
                    deserializer.Configure(config);

                    listener.Prefixes.Add($"http://+:{port}{postRequestTarget}/");
                    listener.Start();

                    Console.WriteLine($"Server is running on port {port}");
                    Console.WriteLine("Press Enter to exit.");

                    // Start a thread to listen for Enter key press
                    Thread exitThread = new Thread(() =>
                    {
                        Console.ReadLine();
                        _keepRunning = false;
                        listener.Stop();
                    });
                    exitThread.Start();

                    // Process incoming requests
                    while (_keepRunning)
                    {
                        try
                        {
                            // Wait for an incoming request
                            var context = await listener.GetContextAsync();

                            // Handle the request asynchronously without blocking
                            _ = Task.Run(() => HandleRequestAsync(context, deserializer, httpTopicHeaderKey));
                        }
                        catch (HttpListenerException)
                        {
                            // Listener was stopped
                            break;
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Error accepting request: {ex.Message}");
                        }
                    }

                    exitThread.Join();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception thrown: {ex.Message}");
                return 1;
            }

            Console.WriteLine("Server shutdown.");
            return 0;
        }

        /// <summary>
        /// Handles an incoming HTTP request.
        /// </summary>
        /// <param name="context">The HTTP listener context</param>
        /// <param name="deserializer">The JSON Schema deserializer</param>
        /// <param name="httpTopicHeaderKey">Optional HTTP header key containing the topic</param>
        private static async Task HandleRequestAsync(HttpListenerContext context, JsonSchemaDeserializer<User> deserializer, string httpTopicHeaderKey)
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
                await HandleSerdesMessageAsync(context, deserializer, httpTopicHeaderKey);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Internal error: {ex.Message}");
                await SendResponseAsync(context, 500, Encoding.UTF8.GetBytes("An internal server error occurred."));
            }
        }

        /// <summary>
        /// Handles a SERDES message by extracting headers, deserializing payload, and sending a response.
        /// </summary>
        /// <param name="context">The HTTP listener context</param>
        /// <param name="deserializer">The JSON Schema deserializer</param>
        /// <param name="httpTopicHeaderKey">Optional HTTP header key containing the topic</param>
        private static async Task HandleSerdesMessageAsync(HttpListenerContext context, JsonSchemaDeserializer<User> deserializer, string httpTopicHeaderKey)
        {
            // Extract topic if header key is configured
            string requestTopic = "";
            if (!string.IsNullOrEmpty(httpTopicHeaderKey))
            {
                requestTopic = context.Request.Headers[httpTopicHeaderKey] ?? "";
            }

            // Read the message body
            byte[] messagePayload;
            using (var ms = new MemoryStream())
            {
                await context.Request.InputStream.CopyToAsync(ms);
                messagePayload = ms.ToArray();
            }

            // Extract SERDES headers from HTTP headers
            var serdesHeaders = ExtractSerdesHeaders(context.Request.Headers);

            // Deserialize the message
            User user = await deserializer.DeserializeAsync(requestTopic, messagePayload, serdesHeaders);

            Console.WriteLine();
            Console.WriteLine("- - - - - - - - - - RECEIVED SERDES MESSAGE - - - - - - - - - -");
            PrintAllHeaders(context.Request.Headers);
            Console.WriteLine($"BodyBytesLength: {messagePayload.Length}");
            Console.WriteLine($"Body: {user}");
            Console.WriteLine("- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -");

            // Send success response
            await SendResponseAsync(context, 200, Array.Empty<byte>());
        }

        /// <summary>
        /// Checks if the request contains SERDES headers.
        /// </summary>
        /// <param name="context">The HTTP listener context</param>
        /// <returns>True if SERDES headers are present</returns>
        private static bool IsSerdesMessage(HttpListenerContext context)
        {
            const string solaceUserPropertyPrefix = "solace-user-property-";

            foreach (string key in context.Request.Headers.AllKeys)
            {
                if (key != null && key.StartsWith(solaceUserPropertyPrefix, StringComparison.OrdinalIgnoreCase))
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
        /// Extracts SERDES headers from NameValueCollection (used with HttpListener).
        /// Parses headers in the format "solace-user-property-*" and extracts value and optional type.
        /// For more details on Solace-User-Property headers, see:
        /// https://docs.solace.com/API/RESTMessagingPrtl/Solace-REST-Message-Encoding.htm#solace-message-custom-properties
        /// </summary>
        /// <param name="headers">The name-value collection of headers</param>
        /// <returns>A dictionary of SERDES headers with parsed values</returns>
        private static Dictionary<string, object> ExtractSerdesHeaders(System.Collections.Specialized.NameValueCollection headers)
        {
            const string solaceUserPropertyPrefixLower = "solace-user-property-";


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
                if (key.StartsWith(solaceUserPropertyPrefixLower, StringComparison.OrdinalIgnoreCase))
                {
                    string httpValue = headers[key];
                    if (string.IsNullOrEmpty(httpValue))
                    {
                        continue;
                    }

                    // Parse value and optional type using regex
                    Match matcher = typePattern.Match(httpValue);
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
                        // Convert to long for int64 type
                        if (long.TryParse(valueAsString, out long longValue))
                        {
                            value = longValue;
                        }
                        else
                        {
                            // Fallback to string if parsing fails
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
                    string propertyKey = key.Substring(solaceUserPropertyPrefixLower.Length).ToLowerInvariant();
                    serdesHeaders[propertyKey] = value;
                }
            }

            return serdesHeaders;
        }
    }
}

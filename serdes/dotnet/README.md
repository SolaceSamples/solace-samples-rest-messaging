This folder contains sample .NET applications that demonstrate how to use the Solace Schema Registry SERDES provider for REST publishers and consumers when connecting to a Schema Registry (by default running on `localhost`).

All of the samples use JSON Schema. The solution file is located at `src/JsonSchema/JsonSchema.sln`.

## Contents

### Solution Projects

The solution (`src/JsonSchema/JsonSchema.sln`) contains the following projects:

| Project | Description |
| --- | --- |
| `RestJsonSchemaPublisher` | Publishes `User` messages over HTTP POST to a Solace broker. The schema ID is sent as a `String` in the `Solace-User-Property-` header. |
| `RestJsonSchemaPublisherWithSchemaId` | Same as `RestJsonSchemaPublisher`, but sends the schema ID as a `Long` (`int64`) using the `; type=int64` header suffix. |
| `RestJsonSchemaConsumer` | Starts a local `HttpListener` that receives messages pushed from a Solace REST Delivery Point (RDP) and deserializes them into `User` objects. |
| `RestJsonSchemaSyncRequestReplyProducer` | Sends a `CreateUser` request over HTTP and synchronously waits for the `CreateUserResponse` in the same HTTP response body. |
| `RestJsonSchemaSyncRequestReplyConsumer` | Receives `CreateUser` requests via `HttpListener`, creates a `CreateUserResponse`, and returns it synchronously in the HTTP response. |
| `RestJsonSchemaAsyncRequestReply` | Publishes a `CreateUser` request and listens on a separate local port for the `CreateUserResponse` delivered asynchronously by an RDP. |
| `Resources` | Shared class library containing the `User`, `CreateUser`, and `CreateUserResponse` POCOs plus their embedded JSON schema files. |

> [!NOTE]
> Due to limitations regarding reply-to-topics in request/reply messaging over REST, there is no .NET sample application for an asynchronous SERDES replier. A synchronous replier (such as `RestJsonSchemaSyncRequestReplyConsumer` in this solution, or the [JsonSchemaSerdesReplier](https://github.com/SolaceSamples/solace-samples-dotnet/tree/master/src/features/serdes/JsonSchema/JsonSchemaSerdesReplier)) must be used.

### Shared Resources

The `Resources` project at `src/Resources/` contains:

- `Serdes/JsonSchema/User.cs`, `CreateUser.cs`, `CreateUserResponse.cs`: Plain .NET model classes used by every sample.
- `Serdes/JsonSchema/Schemas/user.json`, `create-user.json`, `create-user-response.json`: The JSON Schema definitions to be uploaded to the Schema Registry.

## Requirements

- [.NET SDK 8.0](https://dotnet.microsoft.com/download) (the projects also multi-target `net462` for Windows-only builds)
- A running Solace PubSub+ broker with REST messaging enabled
- A running [Solace Schema Registry](https://docs.solace.com/Schema-Registry/schema-registry-overview.htm)

## Solace Schema Registry

For information about how to deploy and configure the Solace Schema Registry, please refer to our documentation here:
https://docs.solace.com/Schema-Registry/schema-registry-overview.htm

## Upload a Schema

Before running the samples, the three JSON Schemas from `src/Resources/Serdes/JsonSchema/Schemas/` must be uploaded to the Schema Registry. To upload each schema:

1. Log into the Schema Registry with an account that has write access and click the "Create Artifact" button.
2. Leave the Group Id field empty.

    ### JSON Schema
    - **Artifact Id** (one per schema, each uploaded separately):
        - For `user.json`, use `solace/samples/json`
        - For `create-user.json`, use `solace/samples/create-user/json`
        - For `create-user-response.json`, use `solace/samples/create-user-response/json`
    - **Type**: Select `JSON Schema`.

> [!NOTE]
> Each schema must be uploaded separately with its own unique Artifact Id to avoid conflicts.

3. Click "Next" to proceed.
4. Skip the Artifact Metadata section and click "Next".
5. On the Version Content Page, leave the version set to auto (or enter a specific value).
6. Upload the matching schema file from `src/Resources/Serdes/JsonSchema/Schemas/`:
    - For Artifact Id `solace/samples/json`, upload `user.json`
    - For Artifact Id `solace/samples/create-user/json`, upload `create-user.json`
    - For Artifact Id `solace/samples/create-user-response/json`, upload `create-user-response.json`
7. Click "Next", skip Version Metadata, then click "Create".

## Broker Setup

### For the Consumer Sample

1. **Create a Queue** subscribed to `solace/samples/json`.
2. **Configure a REST Delivery Point (RDP)**:
    - Create a REST Delivery Point.
    - Create a REST consumer pointing at the host/port where `RestJsonSchemaConsumer` will run.
    - Add a queue binding that references the queue above and set the POST Request Target to match the `<post-request-target>` argument you will pass to the sample (for example, `/message`).

### For the Request/Reply Samples

1. **Create Queues**:
    - A queue subscribed to `solace/samples/create-user/json`
    - A queue subscribed to `solace/samples/create-user-response/json`
2. **Configure a REST Delivery Point** for the reply flow:
    - Create a REST consumer pointing at the host/port of your requesting application.
    - Add a queue binding to the queue subscribed to `solace/samples/create-user-response/json` and set its POST Request Target to match the value your requesting application is listening on (for example, `/message`).

For more detailed RDP documentation, see the [Solace Documentation on REST Delivery Points](https://docs.solace.com/Services/Managing-RDPs.htm?Highlight=rest#configuring-REST-delivery-points).

## Building the Samples

From `src/JsonSchema/`:

```shell
dotnet build JsonSchema.sln
```

To build only a single sample:

```shell
dotnet build RestJsonSchemaPublisher/RestJsonSchemaPublisher.csproj
```

## Running the Samples

Every sample reads its Schema Registry and topic settings from environment variables (see the [Environment Variables](#environment-variables) section below). The defaults assume the Schema Registry is running on `http://localhost:8081` and the topic is `solace/samples/json`.

Run samples with `dotnet run` from within the project directory, or by invoking the built binary directly from `bin/Debug/net8.0/`.

### Publisher

Publishes `User` messages continuously until Enter is pressed. The schema ID is sent as a `String` header.

```shell
cd src/JsonSchema/RestJsonSchemaPublisher
dotnet run -- <host> <port>
# Example: publish to a broker on localhost with REST publishing port 9000
dotnet run -- localhost 9000
```

### Publisher With Schema Id (int64)

Identical behaviour to the publisher above, but the schema ID is written as an `int64` value with a `; type=int64` header suffix.

```shell
cd src/JsonSchema/RestJsonSchemaPublisherWithSchemaId
dotnet run -- <host> <port>
# Example: publish to a broker on localhost with REST publishing port 9000
dotnet run -- localhost 9000
```


### Consumer

Starts a local HTTP server that accepts POSTs from a Solace RDP.

```shell
cd src/JsonSchema/RestJsonSchemaConsumer
dotnet run -- <post-request-target> [<port>] [<http-topic-header-key>]
# Example: listen on http://localhost:8080/message and read the topic from the "X-Solace-Topic" header
dotnet run -- /message 8080 X-Solace-Topic
```

- `<post-request-target>`: Endpoint path the consumer will listen on (must match the POST Request Target configured on the RDP queue binding).
- `<port>`: Local HTTP listen port (defaults to `8080` if omitted).
- `<http-topic-header-key>`: Optional HTTP header that carries the topic name, if your RDP is configured to add one.

> [!NOTE]
> On Windows, `HttpListener` may require the URL prefix to be reserved (run as administrator or use `netsh http add urlacl`). On macOS and Linux this is not needed.

### Synchronous Request/Reply Producer

Sends a `CreateUser` request over REST and reads the `CreateUserResponse` from the HTTP response body.

```shell
cd src/JsonSchema/RestJsonSchemaSyncRequestReplyProducer
dotnet run -- <host> <port>
# Example: send a request to a broker on localhost with REST publishing port 9000
dotnet run -- localhost 9000
```

### Synchronous Request/Reply Consumer

Listens for `CreateUser` requests and replies synchronously in the HTTP response.

```shell
cd src/JsonSchema/RestJsonSchemaSyncRequestReplyConsumer
dotnet run -- <post-request-target> [<port>] [<http-topic-header-key>]
# Example: listen on http://localhost:8080/message and read the topic from the "X-Solace-Topic" header
dotnet run -- /message 8080 X-Solace-Topic
```

### Asynchronous Request/Reply

Publishes a `CreateUser` request over HTTP and concurrently listens on a second port for the `CreateUserResponse` that the broker delivers via an RDP.

```shell
cd src/JsonSchema/RestJsonSchemaAsyncRequestReply
dotnet run -- <host> <publishing-port> <reply-post-request-target> <listen-port> [<http-topic-header-key>]
# Example:
dotnet run -- localhost 9000 /message 38081
```


## Environment Variables

The Schema Registry connection, message content type, and topics can be customized by setting environment variables before launching a sample. If unset, the defaults below are used.

```shell
export REGISTRY_URL="http://localhost:8081/apis/registry/v3"
export REGISTRY_USERNAME="sr-readonly"
export REGISTRY_PASSWORD="roPassword"
export CONTENT_TYPE="BINARY"
# Used by the publisher and consumer samples
export TOPIC="solace/samples/json"
# Used by the request/reply samples
export REQUEST_TOPIC="solace/samples/create-user/json"
export REPLY_TOPIC="solace/samples/create-user-response/json"
```

`CONTENT_TYPE` accepts `BINARY` (mapped to `application/octet-stream`, a Solace binary message) or `JSON` (mapped to `application/json`, a Solace text message). Any other value is passed through as-is as the HTTP `Content-Type`. For details on how HTTP content types map to Solace message types, see the [Solace REST Message Encoding documentation](https://docs.solace.com/API/RESTMessagingPrtl/Solace-REST-Message-Encoding.htm#solace-message-type-mapping-to-http-content-type).

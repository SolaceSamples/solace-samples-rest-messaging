This folder will contain a sample Java application that demonstrates how to use Solace Schema Registry SERDES for REST producers and consumers to connect to the schema registry on localhost.

# Requirements
- Java 11 or later
- Gradle 8.5

1. Get the schema registry package solace-schema-registry-dist from public repo [https://github.com/SolaceLabs/schema-registry-beta/releases](https://github.com/SolaceLabs/schema-registry-beta/releases). Un-Tar it and run the script from this folder.

2. Set up Registry - the setup-registry.js sets up the env vars from .env and runs the docker compose file and does a system health check.
```shell
node setup-registry.js
```
3. Open browser and navigate to http://localhost:8888

4. Login with one of the following:
   * `sr-developer:<devPassword>`
   * `sr-readonly:<roPassword>`

5. Upload a Schema

   To upload a schema, follow these steps:

   1. Begin by logging into an account with write access and click on the "Create Artifact" button.

   2. Leave the Group Id field empty.

   ### Avro Schema
   - **Artifact Id**: Use a unique identifier for each schema:
     - For `user.avsc`, use `solace/samples/avro`
   - **Type**: Select `Avro Schema`.

   ### JSON Schema
   - **Artifact Id**: Use `solace/samples/json` for the `user.json` schema.
   - **Type**: Select `JSON Schema`.

   > **Note:** Each schema must be uploaded separately with its own unique Artifact Id to avoid conflicts.

   After setting the Artifact ID and Type, follow these steps:

   4. Click the "Next" button to proceed.

   5. You can skip the Artifact Metadata section as it's not required. Simply press "Next" to continue.

   6. On the Version Content Page, leave the version set to auto, or if preferred, enter a specific value of your choice.
   
   ### For Avro Schema:
   7. On the Version Content Page, upload the appropriate schema file from the `rest/java/src/main/resources/avro-schema/` directory:
      - When using Artifact Id `solace/samples/avro`, upload `user.avsc`

   ### For JSON Schema:
   7. On the Version Content Page, upload the appropriate schema file from the `rest/java/src/main/resources/json-schema/` directory:
      - When using Artifact Id `solace/samples/json`, upload `user.json`

   8. Click "Next" to move forward.

   9. The Version Metadata is not necessary and can be skipped.

    10. Finally, click the "Create" button to complete the process.

6. Add the required Jars to the samples
   1. Create a `lib` folder.
   2. Extract the zip archives for the Solace Avro SERDES and Solace Json Schema SERDES.
   3. Identify all JAR files within the extracted folders. They are under the `lib` folders.
   4. Move these JAR files into the newly created `lib` directory.


7. Build the java samples
NOTE: For windows users, use the `gradlew.bat` file instead of `gradlew` below
```shell
./gradlew build
```

8. Run a java sample
```shell
./gradlew run<SAMPLE_CLASS_NAME> --args="<CMD LINE ARGS HERE>"
# For example running JsonSchemaRestPublisherHttpClient against a broker located at brokerUrl with a REST publishing port of 38080
./gradlew runJsonSchemaRestPublisherHttpClient --args="brokerUrl 38080"
```

### Running the REST Consumer Samples

The REST consumer samples start a local HTTP server that listens for incoming POST requests from a Solace broker's REST Delivery Point (RDP).

To run a consumer sample, use the following command:
```shell
./gradlew run<CONSUMER_SAMPLE_CLASS_NAME> --args="<post-request-target> <port> [<http-topic-header-key>]"
```

-   `<post-request-target>`: The endpoint path that the consumer will listen on (e.g., `/my-rest-endpoint`). This must match the "POST Request Target" configured in your RDP.
-   `<port>`: The port number for the local HTTP server (e.g., `8080`).
-   `[<http-topic-header-key>]`: (Optional) The name of a custom HTTP header that contains the message's topic. This is only needed if your RDP is configured to add the topic to a specific header.

**Example:**
To run the `JsonSchemaRestConsumer` on port `8080`, listening on `/my-rest-endpoint`, and expecting the topic in the `X-Solace-Topic` header, use:
```shell
./gradlew runJsonSchemaRestConsumer --args="/my-rest-endpoint 8080 X-Solace-Topic"
```

To deliver messages to the running consumer, you must configure a REST Delivery Point (RDP) on your Solace broker. For detailed instructions, refer to the [Solace Documentation on REST Delivery Points](https://docs.solace.com/Services/Managing-RDPs.htm?Highlight=rest#configuring-REST-delivery-points).

NOTE: The registry URL, username, password and content-type can be customized by setting environment variables. 
If not set, the application will use default values. 
To override the defaults, set the following environment variables before running the application:
The values shown below are the default settings. Modify these as needed for your specific registry configuration.
```shell
export REGISTRY_URL="http://localhost:8081/apis/registry/v3"
export REGISTRY_USERNAME="sr-readonly"
export REGISTRY_PASSWORD="roPassword"
export CONTENT_TYPE="BINARY"
# For JSON Schema samples
export TOPIC="solace/samples/json"
export REQUEST_TOPIC="solace/samples/create-user/json"
export REPLY_TOPIC="solace/samples/create-user-response/json"
# For Avro samples
export TOPIC="solace/samples/avro"
export REQUEST_TOPIC="solace/samples/create-user/avro"
export REPLY_TOPIC="solace/samples/create-user-response/avro"
```

# Enabling Network-Level Debug Logging

The Solace Schema Registry Serdes provider uses the Vert.x framework for making REST API calls to the Schema Registry. Vert.x has its own logging abstraction layer and will automatically detect and delegate to a logging backend on the classpath. The order of preference is SLF4J, Log4j2, and then Java Util Logging (JUL). For more information on Vert.x logging, take a look at the documentation https://vertx.io/docs/vertx-core/java/#_logging.

This project is configured to use **Log4j2**. To enable detailed network-level logging, you can adjust the logging levels in the `rest/java/src/main/resources/log4j2.xml` file. While Vert.x is used for handling REST API calls, it is built on top of the Netty project for its low-level network operations. Therefore, to see the raw network traffic, you should enable logging for the `io.netty` package.

1.  Open the `rest/java/src/main/resources/log4j2.xml` file.

2.  To get detailed logs for network traffic, you can set the `io.netty` logger level to `DEBUG`. For example:

    ```xml
    <Loggers>
        <!-- Set specific loggers to DEBUG to see detailed output -->
        <Logger name="io.netty" level="debug" additivity="false">
            <AppenderRef ref="Console"/>
        </Logger>
        
        <!-- Keep the root logger at ERROR to avoid excessive logging from other libraries -->
        <Root level="error">
            <AppenderRef ref="Console"/>
        </Root>
    </Loggers>
    ```

3.  If you want to enable `DEBUG` logging for all libraries, you can change the root level:
    
    ```xml
    <Root level="debug">
        <AppenderRef ref="Console"/>
    </Root>
    ```

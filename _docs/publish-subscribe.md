---
layout: tutorials
title: Publish / Subscribe
summary: Learn how pub/sub using Solace REST Messaging with a Solace VMR.
icon: I_dev_P+S.svg
---

This tutorial will introduce you to the fundamentals of the Solace REST messaging API. The tutorial will show you how to connecting a client, sending a message on a topic subscription and receive this message again through the Solace REST messaging API. This forms the basis for any publish / subscribe message exchange illustrated here:  

## Assumptions

This tutorial assumes the following:

*   You are familiar with Solace [core concepts]({{ site.docs-core-concepts }}){:target="_top"}.
*   You have access to a running Solace message router with the following configuration:
    *   Connectivity information for a Solace message-VPN
    *   Enabled client username and password
    *   Enabled guaranteed messaging support (needed for REST consumers)
    *   Client-profile enabled with guaranteed messaging permissions.

*   REST service enabled for incoming and outgoing messages

One simple way to get access to Solace messaging quickly is to create a messaging service in Solace Cloud [as outlined here]({{ site.links-solaceCloud-setup}}){:target="_top"}. You can find other ways to get access to Solace messaging below.

You can learn all about REST on Solace messaging by referring to the [Online REST Messaging Documentation]({{ site.docs-rest-messaging }}){:target="_top"}.

## Goals

The goal of this tutorial is to demonstrate the most basic messaging interaction using Solace. This tutorial will show you:

1.  How to send a message on a topic using the Solace REST messaging API
2.  How to receive a message using the Solace REST messaging API

## Solace REST Messaging API Introduction

As outlined in the [Online REST Messaging Documentation]({{ site.docs-rest-messaging }}){:target="_top"}, the API enable users to send messages to and asynchronously receive messages with Solace messaging over HTTP using a RESTful API.

The Solace API uses HTTP POST requests to allow clients to publish message Solace messaging. On the subscribe side, the Solace API follows the asynchronous notification pattern and uses an HTTP POST from Solace messaging to the client to delivery messages. This means that pub and sub messages are sent on different HTTP connections than they are received as shown in the following figure.

![solace-rest-messaging-api]({{ site.baseurl }}/assets/images/solace-rest-messaging-api.png)

There are several benefits to this approach. First it removes the possibility of message loss which can exist when using HTTP GET requests without explicit separate acknowledgement. It also enables much higher performance and overall message rate when multiple, parallel HTTP connections are used.

The [Online REST Messaging Documentation]({{ site.docs-rest-messaging }}){:target="_top"} has the following parts which explain the API in more detail:

* REST Messaging Introduction & REST Messaging Concepts which explains the API at an architectural level.
* REST Messaging Protocol which explains the wireline details - like how to format the HTTP messages etc.

Because of the difference between publishing and subscribing, these topics are introduced as needed in the tutorial below.

{% include_relative assets/solaceMessaging.md %}

## Obtaining the Solace API

There is no API to obtain. The Solace REST messaging API is a wireline RESTful HTTP protocol. It is fully outlined in [REST Messaging Protocol]({{ site.docs-rest-protocol }}){:target="_top"}.

## Receiving a message

First this tutorial will show how to setup the subscriber side so that you are ready to receive messages that are published.

![]({{ site.baseurl }}/assets/images/pub-sub-receiving-message-300x134.png)

On the consume side, the Solace REST messaging API depends on a guaranteed messaging queue. As such it is a requirement for REST consumers that Solace messaging support guaranteed messaging and have this feature configured as outlined in the [assumptions section above](#assumptions).

In order to receive REST messages from Solace messaging, you must configure a Guaranteed messaging queue and a REST delivery point. The queue is used to attract messages to the consumer application. The REST delivery point is the Solace message router component that delivers the messages from the queue to the consumer application asynchronously through HTTP POST requests. This is explained in more detail in the [REST Messaging Concepts]({{ site.docs-rest-concepts }}){:target="_top"} where the REST consumers are explained. This tutorial will walk you through the required Solace messaging configuration steps required to create a queue and REST delivery point to connect to your REST consumer application.

### A Simple REST Consumer

First you need a REST consuming application ready to receive HTTP connections from Solace messaging. This can be any HTTP server. This tutorial will demonstrate this using Node.js but Solace REST Messaging uses standard HTTP, so use your favorite HTTP server.

Create a file named NodeRestServer.js with the following contents.

```
var http = require('http');

http.createServer(function (req, res) {
    console.log('Received message: ' + req.url);
    res.writeHead(200);
    res.end();
}).listen(RC_PORT, 'RC_HOST');
console.log('Server running at http://RC_HOST:RC_PORT/');
```

In the above, you need to update RC_HOST and RC_PORT to represent the HOST and PORT that your REST consumer application will be listening on. This HTTP server listens for incoming requests and for each request it will print the URL of the request and respond with a 200 OK response. The 200 OK response indicates to Solace messaging that the message has been successfully processed and it can be removed from Solace messaging queue.

Start your REST consumer using Node.js. For example:

```
$ node NodeRestServer.js
Server running at http://RC_HOST:RC_PORT/
```

**Note:** The executable is `nodejs` on Ubuntu due to a naming conflict with other packages.

Again in your environment, the RC_HOST and RC_PORT will be the host/IP and port that your server is listening on. For example http://192.168.1.110:9090/.



**Note:** Even though this tutorial is illustrating how to publish with direct messages, for REST delivery points, the messages are always consumed from a queue. The incoming messages are promoted into the Solace queue as non-persistent messages and delivered to the REST consumer as non-persistent messages. For more information on this see the [Features – Topic Matching and Message Delivery Modes]({{ site.docs-topic-matching }}){:target="_top"}.

### Configuring a REST Delivery Point

Next, you must configure a queue and REST delivery point on Solace messaging. This means configuring the following Solace messaging components.

<table>
<tr>
    <th>Resource</th>
    <th>Value</th>
  </tr>
  <tr>
    <td>Queue</td>
    <td>Q/rdp1/input</td>
  </tr>
  <tr>
    <td>Pub/Sub Topic</td>
    <td>T/rest/pubsub</td>
  </tr>
  <tr>
    <td>REST Delivery Point</td>
    <td>rdp1</td>
  </tr>
  <tr>
    <td>Queue Binding</td>
    <td>Q/rdp1</td>
  </tr>
  <tr>
    <td>POST Request Target</td>
    <td>/rest/tutorials</td>
  </tr>
  <tr>
    <td>REST Consumer</td>
    <td>rc1</td>
  </tr>
  <tr>
    <td>Remote Host</td>
    <td>RC_HOST – Update to match REST consumer application.</td>
  </tr>
  <tr>
    <td>Remote Port</td>
    <td>RC_PORT – Update to match REST consumer application.</td>
  </tr>
</table>

You can learn about each of these components using [Features – REST Introduction]({{ site.docs-rest-introduction }}){:target="_top"}. In the script below, update VPNNAME to match that of your Solace messaging solution, and the RC_HOST and RC_PORT to match your REST consumer application.

```
home
enable
configure

message-spool message-vpn "VPNNAME"
    ! pragma:interpreter:ignore-already-exists
    create queue "Q/rdp1/input" primary
        access-type "exclusive"
        permission all "delete"
        subscription topic "T/rest/pubsub"
        no shutdown
        exit
    exit

message-vpn "VPNNAME"
    rest
        ! pragma:interpreter:ignore-already-exists
        create rest-delivery-point "rdp1"
            shutdown
            client-profile "default"
            ! pragma:interpreter:ignore-already-exists
            create queue-binding "Q/rdp1/input"
                post-request-target "/rest/tutorials"
                    exit
            ! pragma:interpreter:ignore-already-exists
            create rest-consumer "rc1"
                shutdown
                remote host "RC_HOST"
                remote port "RC_PORT"
                no shutdown
                exit
            no shutdown
            exit
        exit
    exit
end
```

To apply this configuration, simply log in to Solace messaging CLI as an admin user and paste the above script fragments into the CLI.

{% if jekyll.environment == 'solaceCloud' %}

If connecting using Solace Cloud, obtain your management credentials by scrolling down to the Management section on the Connectivity tab, and connect using port 2222.

![]({{ site.baseurl }}/assets/images/management-info.png)

```
ssh <management-username>@<HOST> -p 2222
Solace - Virtual Message Router (VMR)
Password:
```
{% endif %}

If using a VMR load, log in to the Solace message router CLI using the management username and password for your Solace VMR.

```
ssh admin@<HOST>
Solace - Virtual Message Router (VMR)
Password:
```

At this the REST delivery point is configured and should be operational and connected to your REST consumer application. You can verify this using SolAdmin or through the following CLI command.

```
solace(configure)# show message-vpn VPNNAME rest rest-delivery-point *

Total REST Delivery Points (up):                       1
Total REST Delivery Points (configured):               1
Total REST Consumers (up):                             1
Total REST Consumers (configured):                     1
Total REST Consumer Outgoing Connections (up):         3
Total REST Consumer Outgoing Connections (configured): 3
Total Queue Bindings (up):                             1
Total Queue Bindings (configured):                     1

Flags Legend:
A - Admin State (U=Up, D=Down)
O - Oper State (U=Up, D=Down)

                                                  REST
                                                Consumer
                                                Outgoing      Queue       Conns
                                        Status    Conns      Bindings    Blocked

RDP Name             Message VPN         A O    (up/conf)    (up/conf)     (%)
-------------------- ------------------ ------ ----------- ------------- -------
rdp1                 default             U U       3 / 3       1 / 1        0
```

At this point the consumer is up and ready to receive messages.

## Sending a message

Now it is time to send a message to the waiting consumer.  

[]({{ site.baseurl }}/assets/images/pub-sub-sending-message-300x134.png)

Sending a REST message to Solace is very simple. For this example, we will use the command line tool cURL to send the required HTTP. Refer to [REST Messaging Protocol Guide]({{ site.docs-rest-protocol }}){:target="_top"} for the full details of the Solace REST messaging API.

To send a message you can use the following command.

```
$ curl -X POST -d "Hello World REST" http://HOST:PORT/T/rest/pubsub -H "content-type: text" -H "Solace-delivery-mode: direct"
```

You will need to update HOST and PORT to match your Solace messaging HOST and REST service port. This will send a message with contents “Hello World REST” as a Solace text message using the direct delivery mode. The “content-type” headers and “Solace-delivery-mode” are optional. If they are omitted then the Solace REST messaging default delivery mode of “persistent” will be used and the message contents will be treated as binary.

You can also add credentials to the request by updating the cURL command to the following:

```
$ curl -X POST -d "Hello World REST" http://HOST:PORT/T/rest/pubsub -H "content-type: text" -H "Solace-delivery-mode: direct" --user restUsers:restPassword
```

At this point your REST consumer should have received a message. It will print the URL to the screen. So the output should now include:

```
Received message: /rest/tutorials
```

## Summarizing

You have now successfully sent and received a REST message on a topic.

If you have any issues sending and receiving a message, check the [Solace community]({{ site.links-community }}){:target="_top"} for answers to common issues.

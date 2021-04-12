# Qpid protonj2 Imperative Client configuration

This file details various configuration options for the Imperative API based Java client.  Each of the resources
that allow configuration accept a configuration options object that encapsulates all configuration for that specific
resuource.

## Client Options

Before creating a new connection a Client object is created which accept a ClientOptions object to configure it.

+ **ClientOptions.id** Allows configuration of the AMQP Container Id used by newly created Connections, if none is set the Client instance will create a unique Container Id that will be assigned to all new connections.

## Connection Configuration Options

The ConnectionOptions object can be provided to a Client instance when creating a new connection and allows configuration of several different aspects of the resulting Connection instance.

+ **ConnectionOptions.username** User name value used to authenticate the connection
+ **ConnectionOptions.password** The password value used to authenticate the connection
+ **ConnectionOptions.sslEnabled** A connection level convenience option that enables or disables the transport level SSL functionality.  See the connection transport options for more details on SSL configuration, if nothing is configures the connection will atempt to configure the SSL transport using the standard system level configuration properties.
+ **ConnectionOptions.closeTimeout** Timeout value that controls how long the client connection waits on resource closure before returning. By default the client waits 60 seconds for a normal close completion event.
+ **ConnectionOptions.sendTimeout** Timeout value that controls how long the client connection waits on completion of a synchronous message send before returning an error. By default the client will wait indefinitely for a send to complete.
+ **ConnectionOptions.openTimeout** Timeout value that controls how long the client connection waits on the AMQP Open process to complete  before returning with an error. By default the client waits 15 seconds for a connection to be established before failing.
+ **ConnectionOptions.requestTimeout** Timeout value that controls how long the client connection waits on completion of various synchronous interactions, such as initiating or retiring a transaction, before returning an error. Does not affect synchronous message sends. By default the client will wait indefinitely for a request to complete.
+ **ConnectionOptions.drainTimeout** Timeout value that controls how long the client connection waits on completion of a drain request for a Receiver link before failing that request with an error.  By default the client waits 60 seconds for a normal link drained completion event.
+ **ConnectionOptions.virtualHost** The vhost to connect to. Used to populate the Sasl and Open hostname fields. Default is the main hostname from the hostname provided when opening the Connection.
+ **ConnectionOptions.traceFrames** Configure if the newly created connection should enabled AMQP frame tracing to the system output.

### Connection Transport Options

### Connection SSL Options

### Connection Authentication Options

### Connection Automatic Reconnect Options

## Session Configuration Options

+ **SessionOptions.closeTimeout** Timeout value that controls how long the client session waits on resource closure before returning. By default the client uses the matching connection level close timeout option value.
+ **SessionOptions.sendTimeout** Timeout value that sets the Session level default send timeout which can control how long a Sender waits on completion of a synchronous message send before returning an error. By default the client uses the matching connection level send timeout option value.
+ **SessionOptions.openTimeout** Timeout value that controls how long the client Session waits on the AMQP Open process to complete  before returning with an error. By default the client uses the matching connection level close timeout option value.
+ **SessionOptions.requestTimeout** Timeout value that controls how long the client connection waits on completion of various synchronous interactions, such as initiating or retiring a transaction, before returning an error. Does not affect synchronous message sends. By default the client uses the matching connection level request timeout option value.
+ **SessionOptions.drainTimeout** Timeout value that controls how long the Receiver create by this Session waits on completion of a drain request before failing that request with an error.  By default the client uses the matching connection level drain timeout option value.

## Sender Configuration Options

+ **SenderOptions.closeTimeout** Timeout value that controls how long the client Sender waits on resource closure before returning. By default the client uses the matching session level close timeout option value.
+ **SenderOptions.sendTimeout** Timeout value that sets the Sender default send timeout which can control how long a Sender waits on completion of a synchronous message send before returning an error. By default the client uses the matching session level send timeout option value.
+ **SenderOptions.openTimeout** Timeout value that controls how long the client Sender waits on the AMQP Open process to complete  before returning with an error. By default the client uses the matching session level close timeout option value.
+ **SenderOptions.requestTimeout** Timeout value that controls how long the client connection waits on completion of various synchronous interactions, such as initiating or retiring a transaction, before returning an error. Does not affect synchronous message sends. By default the client uses the matching session level request timeout option value.

## Receiver Configuration Options

+ **ReceiverOptions.closeTimeout** Timeout value that controls how long the client session waits on resource closure before returning. By default the client uses the matching session level close timeout option value.
+ **ReceiverOptions.openTimeout** Timeout value that controls how long the client Session waits on the AMQP Open process to complete  before returning with an error. By default the client uses the matching session level close timeout option value.
+ **ReceiverOptions.requestTimeout** Timeout value that controls how long the client Receiver waits on completion of various synchronous interactions, such settlement of a delivery, before returning an error. By default the client uses the matching session level request timeout option value.
+ **ReceiverOptions.drainTimeout** Timeout value that controls how long the Receiver link waits on completion of a drain request before failing that request with an error.  By default the client uses the matching session level drain timeout option value.

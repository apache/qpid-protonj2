# Qpid ProtonJ2 Imperative Client configuration

This file details various configuration options for the Imperative API based Java client.  Each of the resources
that allow configuration accept a configuration options object that encapsulates all configuration for that specific
resuource.

## Client Options

Before creating a new connection a Client object is created which accepts a ClientOptions object to configure it.

     ClientOptions clientOptions = new ClientOptions();
     Client client = Client.create(clientOptions);

The following options are available for configuration when creating a new **Client** instance.

+ **ClientOptions.id** Allows configuration of the AMQP Container Id used by newly created Connections, if none is set the Client instance will create a unique Container Id that will be assigned to all new connections.

## Connection Configuration Options

The ConnectionOptions object can be provided to a Client instance when creating a new connection and allows configuration of several different aspects of the resulting Connection instance.

     ConnectionOptions connectionOptions = new ConnectionOptions();
     connectionOptions.username("user");
     connectionOptions.password("pass");
     Connection connection = client.connect(serverHost, serverPort, connectionOptions);

The following options are available for configuration when creating a new **Connection**.

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

The ConnectionOptions object exposes a set of configuration options for the underlying I/O transport layer known as the TransportOptions which allows for fine grained configuration of network level options.

     ConnectionOptions connectionOptions = new ConnectionOptions();
     connectionOptions.username("user");
     connectionOptions.password("pass");
     connectionOptions.transportOptions.allowNativeIO(false);
     Connection connection = client.connect(serverHost, serverPort, connectionOptions);

The following transport layer options are available for configuration when creating a new **Connection**.

+ **transportOptions.sendBufferSize** default is 64k
+ **transportOptions.receiveBufferSize** default is 64k
+ **transportOptions.trafficClass** default is 0
+ **transportOptions.connectTimeout** default is 60 seconds
+ **transportOptions.soTimeout** default is -1
+ **transportOptions.soLinger** default is -1
+ **transportOptions.tcpKeepAlive** default is false
+ **transportOptions.tcpNoDelay** default is true
+ **transportOptions.allowNativeIO** When true the transport will use a native IO transport implementations such as Epoll or KQueue when available instead of the NIO layer, which can improve performance. Defaults to true.
+ **transportOptions.useWebSockets** should the client use a Web Socket based transport layer when establishing connections, default is false

### Connection SSL Options

If an secure connection is desired the ConnectionOptions exposes another options type for configuring the client for that, the SslOptions.

     ConnectionOptions connectionOptions = new ConnectionOptions();
     connectionOptions.username("user");
     connectionOptions.password("pass");
     connectionOptions.transportOptions.useSsl(true);
     connectionOptions.transportOptions.trustStoreLocation("<path>");
     connectionOptions.transportOptions.trustStorePassword("password");
     Connection connection = client.connect(serverHost, serverPort, connectionOptions);

The following SSL layer options are available for configuration when creating a new **Connection**.

+ **sslOptions.useSsl** Enables or disables the use of the SSL transport layer, default is false.
+ **sslOptions.keyStoreLocation**  default is to read from the system property "javax.net.ssl.keyStore"
+ **sslOptions.keyStorePassword**  default is to read from the system property "javax.net.ssl.keyStorePassword"
+ **sslOptions.trustStoreLocation**  default is to read from the system property "javax.net.ssl.trustStore"
+ **sslOptions.trustStorePassword**  default is to read from the system property "javax.net.ssl.trustStorePassword"
+ **sslOptions.keyStoreType** The type of keyStore being used. Default is to read from the system property "javax.net.ssl.keyStoreType" If not set then default is "JKS".
+ **sslOptions.trustStoreType** The type of trustStore being used. Default is to read from the system property "javax.net.ssl.trustStoreType" If not set then default is "JKS".
+ **sslOptions.storeType** This will set both the keystoreType and trustStoreType to the same value. If not set then the keyStoreType and trustStoreType will default to the values specified above.
+ **sslOptions.contextProtocol** The protocol argument used when getting an SSLContext. Default is TLS, or TLSv1.2 if using OpenSSL.
+ **sslOptions.enabledCipherSuites** The cipher suites to enable, comma separated. No default, meaning the context default ciphers are used. Any disabled ciphers are removed from this.
+ **sslOptions.disabledCipherSuites** The cipher suites to disable, comma separated. Ciphers listed here are removed from the enabled ciphers. No default.
+ **sslOptions.enabledProtocols** The protocols to enable, comma separated. No default, meaning the context default protocols are used. Any disabled protocols are removed from this.
+ **sslOptions.disabledProtocols** The protocols to disable, comma separated. Protocols listed here are removed from the enabled protocols. Default is "SSLv2Hello,SSLv3".
+ **sslOptions.trustAll** Whether to trust the provided server certificate implicitly, regardless of any configured trust store. Defaults to false.
+ **sslOptions.verifyHost** Whether to verify that the hostname being connected to matches with the provided server certificate. Defaults to true.
+ **sslOptions.keyAlias** The alias to use when selecting a keypair from the keystore if required to send a client certificate to the server. No default.
+ **sslOptions.allowNativeSSL** When true the transport will attempt to use native OpenSSL libraries for SSL connections if possible based on the SSL configuration and available OpenSSL libraries on the classpath.

### Connection Automatic Reconnect Options

When creating a new connection it is possible to configure that connection to perform automatic connection recovery.

     ConnectionOptions connectionOptions = new ConnectionOptions();
     connectionOptions.username("user");
     connectionOptions.password("pass");
     connectionOptions.reconnectionOptions.reconnectEnabled(true);
     connectionOptions.reconnectionOptions.reconnectDelay(10_000);
     connectionOptions.reconnectionOptions.useReconnectBackOff(true);
     connectionOptions.reconnectionOptions.addReconnectHost("<host-name>", 5672);
     Connection connection = client.connect(serverHost, serverPort, connectionOptions);

The following connection automatic reconnect options are available for configuration when creating a new **Connection**.

* **reconnectionOptions.reconnectEnabled** enables connection level reconnect for the client, default is false.
+ **reconnectionOptions.reconnectDelay** Controls the delay between successive reconnection attempts, defaults to 10 milliseconds.  If the backoff option is not enabled this value remains constant.
+ **reconnectionOptions.maxReconnectDelay** The maximum time that the client will wait before attempting a reconnect.  This value is only used when the backoff feature is enabled to ensure that the delay doesn't not grow too large.  Defaults to 30 seconds as the max time between connect attempts.
+ **reconnectionOptions.useReconnectBackOff** Controls whether the time between reconnection attempts should grow based on a configured multiplier.  This option defaults to true.
+ **reconnectionOptions.reconnectBackOffMultiplier** The multiplier used to grow the reconnection delay value, defaults to 2.0d.
+ **reconnectionOptions.maxReconnectAttempts** The number of reconnection attempts allowed before reporting the connection as failed to the client.  The default is no limit or (-1).
+ **reconnectionOptions.maxInitialConnectionAttempts** For a client that has never connected to a remote peer before this option control how many attempts are made to connect before reporting the connection as failed.  The default is to use the value of maxReconnectAttempts.
+ **reconnectionOptions.warnAfterReconnectAttempts** Controls how often the client will log a message indicating that failover reconnection is being attempted.  The default is to log every 10 connection attempts.
+ **reconnectionOptions.addReconnectHost** Allows additional remote peers to be configured for use when the original connection fails or cannot be established to the host provided in the **connect** call.

## Session Configuration Options

When creating a new Session the **SessionOptions** object can be provided which allows some control over various behaviors of the session.

     SessionOptions sessionOptions = new SessionOptions();
     Session session = connection.openSession(sessionOptions);

The following options are available for configuration when creating a new **Session**.

+ **SessionOptions.closeTimeout** Timeout value that controls how long the client session waits on resource closure before returning. By default the client uses the matching connection level close timeout option value.
+ **SessionOptions.sendTimeout** Timeout value that sets the Session level default send timeout which can control how long a Sender waits on completion of a synchronous message send before returning an error. By default the client uses the matching connection level send timeout option value.
+ **SessionOptions.openTimeout** Timeout value that controls how long the client Session waits on the AMQP Open process to complete  before returning with an error. By default the client uses the matching connection level close timeout option value.
+ **SessionOptions.requestTimeout** Timeout value that controls how long the client connection waits on completion of various synchronous interactions, such as initiating or retiring a transaction, before returning an error. Does not affect synchronous message sends. By default the client uses the matching connection level request timeout option value.
+ **SessionOptions.drainTimeout** Timeout value that controls how long the Receiver create by this Session waits on completion of a drain request before failing that request with an error.  By default the client uses the matching connection level drain timeout option value.

## Sender Configuration Options

When creating a new Sender the **SenderOptions** object can be provided which allows some control over various behaviors of the sender.

     SenderOptions senderOptions = new SenderOptions();
     Sender sender = session.openSender("address", senderOptions);

The following options are available for configuration when creating a new **Sender**.

+ **SenderOptions.closeTimeout** Timeout value that controls how long the client Sender waits on resource closure before returning. By default the client uses the matching session level close timeout option value.
+ **SenderOptions.sendTimeout** Timeout value that sets the Sender default send timeout which can control how long a Sender waits on completion of a synchronous message send before returning an error. By default the client uses the matching session level send timeout option value.
+ **SenderOptions.openTimeout** Timeout value that controls how long the client Sender waits on the AMQP Open process to complete  before returning with an error. By default the client uses the matching session level close timeout option value.
+ **SenderOptions.requestTimeout** Timeout value that controls how long the client connection waits on completion of various synchronous interactions, such as initiating or retiring a transaction, before returning an error. Does not affect synchronous message sends. By default the client uses the matching session level request timeout option value.

## Receiver Configuration Options

When creating a new Receiver the **ReceiverOptions** object can be provided which allows some control over various behaviors of the receiver.

     ReceiverOptions receiverOptions = new ReceiverOptions();
     Receiver receiver = session.openReceiver("address", receiverOptions);

The following options are available for configuration when creating a new **Receiver**.

+ **ReceiverOptions.creditWindow** Configures the size of the credit window the Receiver will open with the remote which the Receiver will replenish automatically as incoming deliveries are read.  The default value is 10, to disable and control credit manually this value should be set to zero.
+ **ReceiverOptions.closeTimeout** Timeout value that controls how long the client session waits on resource closure before returning. By default the client uses the matching session level close timeout option value.
+ **ReceiverOptions.openTimeout** Timeout value that controls how long the client Session waits on the AMQP Open process to complete  before returning with an error. By default the client uses the matching session level close timeout option value.
+ **ReceiverOptions.requestTimeout** Timeout value that controls how long the client Receiver waits on completion of various synchronous interactions, such settlement of a delivery, before returning an error. By default the client uses the matching session level request timeout option value.
+ **ReceiverOptions.drainTimeout** Timeout value that controls how long the Receiver link waits on completion of a drain request before failing that request with an error.  By default the client uses the matching session level drain timeout option value.

## Stream Sender Configuration Options

When creating a new Sender the **SenderOptions** object can be provided which allows some control over various behaviors of the sender.

     StreamSenderOptions streamSenderOptions = new StreamSenderOptions();
     StreamSender streamSender = connection.openStreamSender("address", streamSenderOptions);

The following options are available for configuration when creating a new **StreamSender**.

+ **StreamSenderOptions.closeTimeout** Timeout value that controls how long the client Sender waits on resource closure before returning. By default the client uses the matching session level close timeout option value.
+ **StreamSenderOptions.sendTimeout** Timeout value that sets the Sender default send timeout which can control how long a Sender waits on completion of a synchronous message send before returning an error. By default the client uses the matching session level send timeout option value.
+ **StreamSenderOptions.openTimeout** Timeout value that controls how long the client Sender waits on the AMQP Open process to complete  before returning with an error. By default the client uses the matching session level close timeout option value.
+ **StreamSenderOptions.requestTimeout** Timeout value that controls how long the client connection waits on completion of various synchronous interactions, such as initiating or retiring a transaction, before returning an error. Does not affect synchronous message sends. By default the client uses the matching session level request timeout option value.

## Stream Receiver Configuration Options

When creating a new Receiver the **ReceiverOptions** object can be provided which allows some control over various behaviors of the receiver.

     StreamReceiverOptions streamReceiverOptions = new StreamReceiverOptions();
     StreamReceiver streamReceiver = connection.openStreamReceiver("address", streamReceiverOptions);

The following options are available for configuration when creating a new **StreamReceiver**.

+ **StreamReceiverOptions.creditWindow** Configures the size of the credit window the Receiver will open with the remote which the Receiver will replenish automatically as incoming deliveries are read.  The default value is 10, to disable and control credit manually this value should be set to zero.
+ **StreamReceiverOptions.closeTimeout** Timeout value that controls how long the client session waits on resource closure before returning. By default the client uses the matching session level close timeout option value.
+ **StreamReceiverOptions.openTimeout** Timeout value that controls how long the client Session waits on the AMQP Open process to complete  before returning with an error. By default the client uses the matching session level close timeout option value.
+ **StreamReceiverOptions.requestTimeout** Timeout value that controls how long the client Receiver waits on completion of various synchronous interactions, such settlement of a delivery, before returning an error. By default the client uses the matching session level request timeout option value.
+ **StreamReceiverOptions.drainTimeout** Timeout value that controls how long the Receiver link waits on completion of a drain request before failing that request with an error.  By default the client uses the matching session level drain timeout option value.

## Logging

The client makes use of the SLF4J API, allowing users to select a particular logging implementation based on their needs by supplying a SLF4J 'binding', such as *slf4j-log4j* in order to use Log4J. More details on SLF4J are available from http://www.slf4j.org/.

The client uses Logger names residing within the *org.apache.qpid.protonj2* hierarchy, which you can use to configure a logging implementation based on your needs.

When debugging some issues, it may sometimes be useful to enable additional protocol trace logging from the Qpid Proton AMQP 1.0 library. There are two options to achieve this:

+ Set the environment variable (not Java system property) *PN_TRACE_FRM* to *true*, which will cause Proton to emit frame logging to stdout.
+ Setting the option *ConnectionOptions.traceFrames=true* to your connection options object to have the client add a protocol tracer to Proton, and configure the *org.apache.qpid.protonj2.engine.impl.ProtonFrameLoggingHandler* Logger to *TRACE* level to include the output in your logs.

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.protonj2.client.impl;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.Principal;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ConnectionEvent;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.ErrorCondition;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.ReceiverOptions;
import org.apache.qpid.protonj2.client.Sender;
import org.apache.qpid.protonj2.client.SenderOptions;
import org.apache.qpid.protonj2.client.Session;
import org.apache.qpid.protonj2.client.SessionOptions;
import org.apache.qpid.protonj2.client.StreamReceiver;
import org.apache.qpid.protonj2.client.StreamReceiverOptions;
import org.apache.qpid.protonj2.client.StreamSender;
import org.apache.qpid.protonj2.client.StreamSenderOptions;
import org.apache.qpid.protonj2.client.Tracker;
import org.apache.qpid.protonj2.client.exceptions.ClientConnectionRemotelyClosedException;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIOException;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.client.exceptions.ClientOperationTimedOutException;
import org.apache.qpid.protonj2.client.exceptions.ClientUnsupportedOperationException;
import org.apache.qpid.protonj2.client.futures.ClientFuture;
import org.apache.qpid.protonj2.client.futures.ClientFutureFactory;
import org.apache.qpid.protonj2.client.transport.NettyIOContext;
import org.apache.qpid.protonj2.client.transport.Transport;
import org.apache.qpid.protonj2.client.util.TrackableThreadFactory;
import org.apache.qpid.protonj2.engine.Engine;
import org.apache.qpid.protonj2.engine.EngineFactory;
import org.apache.qpid.protonj2.engine.sasl.client.SaslAuthenticator;
import org.apache.qpid.protonj2.engine.sasl.client.SaslCredentialsProvider;
import org.apache.qpid.protonj2.engine.sasl.client.SaslMechanismSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Connection} implementation that uses the Proton engine for AMQP protocol support.
 */
public class ClientConnection implements Connection {

    private static final Logger LOG = LoggerFactory.getLogger(ClientConnection.class);

    // Future tracking of Closing. Closed. Failed state vs just simple boolean is intended here
    // later on we may decide this is overly optimized.
    private static final AtomicIntegerFieldUpdater<ClientConnection> CLOSED_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(ClientConnection.class, "closed");
    private static final AtomicReferenceFieldUpdater<ClientConnection, ClientException> FAILURE_CAUSE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(ClientConnection.class, ClientException.class, "failureCause");

    private final ClientInstance client;
    private final ConnectionOptions options;
    private final ClientConnectionCapabilities capabilities = new ClientConnectionCapabilities();
    private final ClientFutureFactory futureFactory;
    private final ClientSessionBuilder sessionBuilder;
    private final NettyIOContext ioContext;
    private final String connectionId;
    private final ScheduledExecutorService executor;
    private final Map<ClientFuture<?>, Object> requests = new ConcurrentHashMap<>();
    private final ThreadPoolExecutor notifications;

    private Engine engine;
    private org.apache.qpid.protonj2.engine.Connection protonConnection;
    private ClientSession connectionSession;
    private ClientSender connectionSender;
    private Transport transport;
    private ClientFuture<Connection> openFuture;
    private ClientFuture<Connection> closeFuture;
    private volatile int closed;
    private volatile ClientException failureCause;
    private final String host;
    private final int port;

    /**
     * Create a connection and define the initial configuration used to manage the
     * connection to the remote.
     *
     * @param host
     * 		the host that this connection is connecting to.
     * @param port
     * 		the port on the remote host where this connection attaches.
     * @param client
     *      the {@link Client} that this connection resides within.
     * @param options
     *      the connection options that configure this {@link Connection} instance.
     */
    ClientConnection(ClientInstance client, String host, int port, ConnectionOptions options) {
        this.client = client;
        this.options = options;
        this.connectionId = client.nextConnectionId();
        this.futureFactory = ClientFutureFactory.create(client.options().futureType());
        this.openFuture = futureFactory.createFuture();
        this.closeFuture = futureFactory.createFuture();
        this.sessionBuilder = new ClientSessionBuilder(this);
        this.ioContext = new NettyIOContext(options.transportOptions(),
                                            options.sslOptions(),
                                            "ClientConnection :(" + connectionId + "): I/O Thread");
        this.executor = ioContext.eventLoop();
        this.host = host;
        this.port = port;

        // This executor can be used for dispatching asynchronous tasks that might block or result
        // in reentrant calls to this Connection that could block.
        notifications = new ThreadPoolExecutor(1, 1, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>(),
            new TrackableThreadFactory("QpidJMS Connection Executor: " + getId(), true));
        notifications.setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardOldestPolicy());
    }

    @Override
    public ClientInstance client() {
        return client;
    }

    @Override
    public Future<Connection> openFuture() {
        return openFuture;
    }

    @Override
    public void close() {
        try {
            doClose(null).get();
        } catch (InterruptedException | ExecutionException e) {
            Thread.interrupted();
        }
    }

    @Override
    public void close(ErrorCondition error) {
        try {
            doClose(error).get();
        } catch (InterruptedException | ExecutionException e) {
            Thread.interrupted();
        }
    }

    @Override
    public Future<Connection> closeAsync() {
        return doClose(null);
    }

    @Override
    public Future<Connection> closeAsync(ErrorCondition error) {
        Objects.requireNonNull(error, "Error supplied cannot be null");

        return doClose(error);
    }

    private Future<Connection> doClose(ErrorCondition error) {
        if (CLOSED_UPDATER.compareAndSet(this, 0, 1)) {
            try {
                executor.execute(() -> {
                    LOG.trace("Close requested for connection: {}", this);

                    if (protonConnection.isLocallyOpen()) {
                        protonConnection.setCondition(ClientErrorCondition.asProtonErrorCondition(error));

                        try {
                            protonConnection.close();
                        } catch (Throwable ignored) {
                            // Engine error handler will kick in if the write of Close fails
                        }
                    } else {
                        engine.shutdown();
                    }
                });
            } catch (RejectedExecutionException rje) {
                LOG.trace("Close task rejected from the event loop", rje);
            } finally {
                try {
                    closeFuture.get();
                } catch (InterruptedException | ExecutionException e) {
                    // Ignore error as we are closed regardless
                } finally {
                    try {
                        transport.close();
                    } catch (Exception ignore) {}

                    ioContext.shutdown();
                }
            }
        }

        return closeFuture;
    }

    @Override
    public Session defaultSession() throws ClientException {
        checkClosedOrFailed();
        final ClientFuture<Session> defaultSession = getFutureFactory().createFuture();

        executor.execute(() -> {
            try {
                checkClosedOrFailed();
                defaultSession.complete(lazyCreateConnectionSession());
            } catch (Throwable error) {
                defaultSession.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return request(this, defaultSession);
    }

    @Override
    public Session openSession() throws ClientException {
        return openSession(null);
    }

    @Override
    public Session openSession(SessionOptions sessionOptions) throws ClientException {
        checkClosedOrFailed();
        final ClientFuture<Session> createSession = getFutureFactory().createFuture();

        executor.execute(() -> {
            try {
                checkClosedOrFailed();
                createSession.complete(sessionBuilder.session(sessionOptions).open());
            } catch (Throwable error) {
                createSession.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return request(this, createSession);
    }

    @Override
    public Receiver openReceiver(String address) throws ClientException {
        return openReceiver(address, null);
    }

    @Override
    public Receiver openReceiver(String address, ReceiverOptions receiverOptions) throws ClientException {
        checkClosedOrFailed();
        Objects.requireNonNull(address, "Cannot create a receiver with a null address");
        final ClientFuture<Receiver> createReceiver = getFutureFactory().createFuture();

        executor.execute(() -> {
            try {
                checkClosedOrFailed();
                createReceiver.complete(lazyCreateConnectionSession().internalOpenReceiver(address, receiverOptions));
            } catch (Throwable error) {
                createReceiver.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return request(this, createReceiver);
    }

    @Override
    public Receiver openDurableReceiver(String address, String subscriptionName) throws ClientException {
        return openDurableReceiver(address, subscriptionName, null);
    }

    @Override
    public Receiver openDurableReceiver(String address, String subscriptionName, ReceiverOptions receiverOptions) throws ClientException {
        checkClosedOrFailed();
        Objects.requireNonNull(address, "Cannot create a receiver with a null address");
        final ClientFuture<Receiver> createReceiver = getFutureFactory().createFuture();

        executor.execute(() -> {
            try {
                checkClosedOrFailed();
                createReceiver.complete(lazyCreateConnectionSession().internalOpenDurableReceiver(address, subscriptionName, receiverOptions));
            } catch (Throwable error) {
                createReceiver.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return request(this, createReceiver);
    }

    @Override
    public Receiver openDynamicReceiver() throws ClientException {
        return openDynamicReceiver(null, null);
    }

    @Override
    public Receiver openDynamicReceiver(Map<String, Object> dynamicNodeProperties) throws ClientException {
        return openDynamicReceiver(null, null);
    }

    @Override
    public Receiver openDynamicReceiver(ReceiverOptions receiverOptions) throws ClientException {
        return openDynamicReceiver(null, null);
    }

    @Override
    public Receiver openDynamicReceiver(Map<String, Object> dynamicNodeProperties, ReceiverOptions receiverOptions) throws ClientException {
        checkClosedOrFailed();
        final ClientFuture<Receiver> createReceiver = getFutureFactory().createFuture();

        executor.execute(() -> {
            try {
                checkClosedOrFailed();
                createReceiver.complete(lazyCreateConnectionSession().internalOpenDynamicReceiver(dynamicNodeProperties, receiverOptions));
            } catch (Throwable error) {
                createReceiver.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return request(this, createReceiver);
    }

    @Override
    public StreamReceiver openStreamReceiver(String address) throws ClientException {
        return openStreamReceiver(address, null);
    }

    @Override
    public StreamReceiver openStreamReceiver(String address, StreamReceiverOptions receiverOptions) throws ClientException {
        checkClosedOrFailed();
        final ClientFuture<StreamReceiver> createRequest = getFutureFactory().createFuture();

        executor.execute(() -> {
            try {
                // TODO: How to handle session capacity less than max frame size configuration ?
                int sessionCapacity = StreamReceiverOptions.DEFAULT_READ_BUFFER_SIZE;
                if (receiverOptions != null) {
                    sessionCapacity = receiverOptions.readBufferSize() / 2;
                }

                checkClosedOrFailed();
                SessionOptions sessionOptions = new SessionOptions(sessionBuilder.getDefaultSessionOptions());
                ClientSession session = sessionBuilder.session(sessionOptions.incomingCapacity(sessionCapacity)).open();
                createRequest.complete(session.internalOpenStreamReceiver(address, receiverOptions));
            } catch (Throwable error) {
                createRequest.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return request(this, createRequest);
    }

    @Override
    public Sender defaultSender() throws ClientException {
        checkClosedOrFailed();
        final ClientFuture<Sender> defaultSender = getFutureFactory().createFuture();

        executor.execute(() -> {
            try {
                checkClosedOrFailed();
                defaultSender.complete(lazyCreateConnectionSender());
            } catch (Throwable error) {
                defaultSender.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return request(this, defaultSender);
    }

    @Override
    public Sender openSender(String address) throws ClientException {
        return openSender(address, null);
    }

    @Override
    public Sender openSender(String address, SenderOptions senderOptions) throws ClientException {
        checkClosedOrFailed();
        Objects.requireNonNull(address, "Cannot create a sender with a null address");
        final ClientFuture<Sender> createSender = getFutureFactory().createFuture();

        executor.execute(() -> {
            try {
                checkClosedOrFailed();
                createSender.complete(lazyCreateConnectionSession().internalOpenSender(address, senderOptions));
            } catch (Throwable error) {
                createSender.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return request(this, createSender);
    }

    @Override
    public Sender openAnonymousSender() throws ClientException {
        return openAnonymousSender(null);
    }

    @Override
    public Sender openAnonymousSender(SenderOptions senderOptions) throws ClientException {
        checkClosedOrFailed();
        final ClientFuture<Sender> createRequest = getFutureFactory().createFuture();

        executor.execute(() -> {
            try {
                checkClosedOrFailed();
                createRequest.complete(lazyCreateConnectionSession().internalOpenAnonymousSender(senderOptions));
            } catch (Throwable error) {
                createRequest.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return request(this, createRequest);
    }

    @Override
    public StreamSender openStreamSender(String address) throws ClientException {
        return openStreamSender(address, null);
    }

    @Override
    public StreamSender openStreamSender(String address, StreamSenderOptions senderOptions) throws ClientException {
        checkClosedOrFailed();
        Objects.requireNonNull(address, "Cannot create a sender with a null address");
        final ClientFuture<StreamSender> createRequest = getFutureFactory().createFuture();

        executor.execute(() -> {
            try {
                checkClosedOrFailed();
                ClientSession session = sessionBuilder.session(null).open();
                createRequest.complete(session.internalOpenStreamSender(address, senderOptions));
            } catch (Throwable error) {
                createRequest.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return request(this, createRequest);
    }

    @Override
    public Tracker send(Message<?> message) throws ClientException {
        checkClosedOrFailed();
        Objects.requireNonNull(message, "Cannot send a null message");
        final ClientFuture<Tracker> result = getFutureFactory().createFuture();

        executor.execute(() -> {
            try {
                checkClosedOrFailed();
                result.complete(lazyCreateConnectionSender().send(message));
            } catch (Throwable error) {
                result.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return request(this, result);
    }

    @Override
    public Map<String, Object> properties() throws ClientException {
        waitForOpenToComplete();
        return ClientConversionSupport.toStringKeyedMap(protonConnection.getRemoteProperties());
    }

    @Override
    public String[] offeredCapabilities() throws ClientException {
        waitForOpenToComplete();
        return ClientConversionSupport.toStringArray(protonConnection.getRemoteOfferedCapabilities());
    }

    @Override
    public String[] desiredCapabilities() throws ClientException {
        waitForOpenToComplete();
        return ClientConversionSupport.toStringArray(protonConnection.getRemoteDesiredCapabilities());
    }

    @Override
    public String toString() {
        return "ClientConnection:[" + getId() + "]";
    }

    //----- Internal API

    String getId() {
        return connectionId;
    }

    ClientConnection connect() throws ClientException {
        try {
            // Initial configuration validation happens here, if this step fails then the
            // user most likely configured something incorrect or that violates some constraint
            // like an invalid SASL mechanism etc.
            initializeProtonResources(host);
            scheduleConnect(host, port);

            return this;
        } catch (Exception ex) {
            CLOSED_UPDATER.set(this, 1);
            FAILURE_CAUSE_UPDATER.compareAndSet(this, null, ClientExceptionSupport.createOrPassthroughFatal(ex));
            openFuture.failed(failureCause);
            closeFuture.complete(this);
            ioContext.shutdown();

            throw failureCause;
        }
    }

    boolean isClosed() {
        return closed > 0;
    }

    ScheduledExecutorService getScheduler() {
        return executor;
    }

    ClientFutureFactory getFutureFactory() {
        return futureFactory;
    }

    ConnectionOptions getOptions() {
        return options;
    }

    ClientConnectionCapabilities getCapabilities() {
        return capabilities;
    }

    org.apache.qpid.protonj2.engine.Connection getProtonConnection() {
        return protonConnection;
    }

    <T> T request(Object requestor, ClientFuture<T> request) throws ClientException {
        requests.put(request, requestor);

        try {
            return request.get();
        } catch (Throwable error) {
            request.cancel(false);
            throw ClientExceptionSupport.createNonFatalOrPassthrough(error);
        } finally {
            requests.remove(request);
        }
    }

    void failAllPendingRequests(Object requestor, ClientException cause) {
        requests.entrySet().removeIf(entry -> {
            if (entry.getValue() == requestor) {
                entry.getKey().failed(cause);
                return true;
            }

            return false;
        });
    }

    //----- Private implementation events handlers and utility methods

    private void handleLocalOpen(org.apache.qpid.protonj2.engine.Connection connection) {
        connection.tickAuto(getScheduler());

        if (options.openTimeout() > 0) {
            executor.schedule(() -> {
                if (!openFuture.isDone()) {
                    // Ensure a close write is attempted and then force failure regardless
                    // as we don't expect the remote to respond given it hasn't done so yet.
                    try {
                        connection.close();
                    } catch (Throwable ignore) {}

                    connection.getEngine().engineFailed(new ClientOperationTimedOutException(
                        "Connection Open timed out waiting for remote to open"));
                }
            }, options.openTimeout(), TimeUnit.MILLISECONDS);
        }
    }

    private void handleLocalClose(org.apache.qpid.protonj2.engine.Connection connection) {
        if (connection.isRemotelyClosed()) {
            final ClientException failureCause;

            if (engine.connection().getRemoteCondition() != null) {
                failureCause = ClientExceptionSupport.convertToConnectionClosedException(connection.getRemoteCondition());
            } else {
                failureCause = new ClientConnectionRemotelyClosedException("Unknown error led to connection disconnect");
            }

            try {
                connection.getEngine().engineFailed(failureCause);
            } catch (Throwable ignore) {
            }
        } else if (!engine.isShutdown() || !engine.isFailed()) {
            // Ensure engine gets shut down and future completed if remote doesn't respond.
            executor.schedule(() -> {
                try {
                    connection.getEngine().shutdown();
                } catch (Throwable ignore) {
                }
            }, options.closeTimeout(), TimeUnit.MILLISECONDS);
        }
    }

    private void handleRemoteOpen(org.apache.qpid.protonj2.engine.Connection connection) {
        capabilities.determineCapabilities(connection);
        submitConnectionEvent(options.connectedHandler(), transport.getHost(), transport.getPort(), null);
        openFuture.complete(this);
    }

    private void submitConnectionEvent(BiConsumer<Connection, ConnectionEvent> handler, String host, int port, ClientIOException cause) {
        if (handler != null) {
            try {
                notifications.submit(() -> {
                    try {
                        handler.accept(this, new ConnectionEvent(host, port, cause));
                    } catch (Exception ex) {
                        LOG.trace("User supplied connection life-cycle event handler threw: ", ex);
                    }
                });
            } catch (Exception ex) {
                LOG.trace("Error thrown while attempting to submit event notification ", ex);
            }
        }
    }

    private void handleRemotecClose(org.apache.qpid.protonj2.engine.Connection connection) {
        // When the connection is already locally closed this implies the application requested
        // a close of this connection so this is normal, if not then the remote is closing for
        // some reason and we should react as if the connection has failed which we will determine
        // in the local close handler based on state.
        if (connection.isLocallyClosed()) {
            try {
                connection.getEngine().shutdown();
            } catch (Throwable ignore) {
                LOG.warn("Unexpected exception thrown from engine shutdown: ", ignore);
            }
        } else {
            try {
                connection.close();
            } catch (Throwable ignored) {
                // Engine handlers will ensure we close down if not already locally closed.
            }
        }
    }

    private void handleEngineOutput(ProtonBuffer output) {
        try {
            transport.writeAndFlush(output);
        } catch (IOException e) {
            LOG.debug("Error while writing engine output to transport: ", e.getMessage());
            throw new UncheckedIOException(e);
        }
    }

    /*
     * When an engine fails we check if we can reconnect or not and act accordingly.
     */
    private void handleEngineFailure(Engine engine) {
        final ClientIOException failureCause;

        if (engine.connection().getRemoteCondition() != null) {
            failureCause = ClientExceptionSupport.convertToConnectionClosedException(engine.connection().getRemoteCondition());
        } else if (engine.failureCause() != null) {
            failureCause = ClientExceptionSupport.convertToConnectionClosedException(engine.failureCause());
        } else {
            failureCause = new ClientConnectionRemotelyClosedException("Unknown error led to connection disconnect");
        }

        LOG.trace("Engine reports failure with error: {}", failureCause.getMessage());

        failConnection(failureCause);
    }

    /*
     * Handle normal engine shutdown which should only happen when the connection is closed
     * by the user, all other cases should lead to engine failed event first which will deal
     * with reconnect cases and avoid this event unless reconnect cannot proceed.
     */
    private void handleEngineShutdown(Engine engine) {
        if (engine.failureCause() == null) {
            try {
                protonConnection.close();
            } catch (Exception ignore) {
            }

            try {
                transport.close();
            } catch (Exception ignored) {}

            openFuture.complete(this);
            closeFuture.complete(this);
        }
    }

    private void failConnection(ClientIOException failureCause) {
        FAILURE_CAUSE_UPDATER.compareAndSet(this, null, failureCause);

        try {
            engine.shutdown();
        } catch (Exception ignore) {}

        openFuture.failed(failureCause);
        closeFuture.complete(this);

        submitConnectionEvent(options.failedHandler(), transport.getHost(), transport.getPort(), failureCause);
    }

    private Engine configureEngineSaslSupport() {
        if (options.saslOptions().saslEnabled()) {
            SaslMechanismSelector mechSelector =
                new SaslMechanismSelector(ClientConversionSupport.toSymbolSet(options.saslOptions().allowedMechanisms()));

            engine.saslDriver().client().setListener(new SaslAuthenticator(mechSelector, new SaslCredentialsProvider() {

                @Override
                public String vhost() {
                    return options.virtualHost();
                }

                @Override
                public String username() {
                    return options.user();
                }

                @Override
                public String password() {
                    return options.password();
                }

                @Override
                public Principal localPrincipal() {
                    return transport.getLocalPrincipal();
                }
            }));
        }

        return engine;
    }

    private void initializeProtonResources(String host) throws ClientException {
        if (options.saslOptions().saslEnabled()) {
            engine = EngineFactory.PROTON.createEngine();
        } else {
            engine = EngineFactory.PROTON.createNonSaslEngine();
        }

        if (options.traceFrames()) {
            engine.configuration().setTraceFrames(true);
            if (!engine.configuration().isTraceFrames()) {
                LOG.info("Connection frame tracing was enabled but protocol engine does not support it");
            }
        }

        engine.outputHandler(this::handleEngineOutput)
              .shutdownHandler(this::handleEngineShutdown)
              .errorHandler(this::handleEngineFailure);

        protonConnection = engine.connection();

        if (client.containerId() != null) {
            protonConnection.setContainerId(client.containerId());
        }

        protonConnection.setLinkedResource(this);
        protonConnection.setChannelMax(options.channelMax());
        protonConnection.setMaxFrameSize(options.maxFrameSize());
        protonConnection.setHostname(host);
        protonConnection.setIdleTimeout((int) options.idleTimeout());
        protonConnection.setOfferedCapabilities(ClientConversionSupport.toSymbolArray(options.offeredCapabilities()));
        protonConnection.setDesiredCapabilities(ClientConversionSupport.toSymbolArray(options.desiredCapabilities()));
        protonConnection.setProperties(ClientConversionSupport.toSymbolKeyedMap(options.properties()));
        protonConnection.localOpenHandler(this::handleLocalOpen)
                        .localCloseHandler(this::handleLocalClose)
                        .openHandler(this::handleRemoteOpen)
                        .closeHandler(this::handleRemotecClose);

        configureEngineSaslSupport();
    }

    private ClientSession lazyCreateConnectionSession() throws ClientException {
        if (connectionSession == null) {
            connectionSession = sessionBuilder.session(null).open();
        }

        return connectionSession;
    }

    private Sender lazyCreateConnectionSender() throws ClientException {
        if (connectionSender == null) {
            if (openFuture.isComplete()) {
                checkAnonymousRelaySupported();
            }

            connectionSender = lazyCreateConnectionSession().internalOpenAnonymousSender(null);
            connectionSender.remotelyClosedHandler((sender) -> {
                try {
                    sender.closeAsync();
                } catch (Throwable ignore) {}

                // Clear the old closed sender, a lazy create needs to construct a new sender.
                connectionSender = null;
            });
        }

        return connectionSender;
    }

    void checkAnonymousRelaySupported() throws ClientUnsupportedOperationException {
        if (!capabilities.anonymousRelaySupported()) {
            throw new ClientUnsupportedOperationException("Anonymous relay support not available from this connection");
        }
    }

    protected void checkClosedOrFailed() throws ClientException {
        if (closed > 0) {
            throw new ClientIllegalStateException("The Connection was explicity closed", failureCause);
        } else if (failureCause != null) {
            throw failureCause;
        }
    }

    private void waitForOpenToComplete() throws ClientException {
        if (!openFuture.isComplete() || openFuture.isFailed()) {
            try {
                openFuture.get();
            } catch (ExecutionException | InterruptedException e) {
                Thread.interrupted();
                if (failureCause != null) {
                    throw failureCause;
                } else {
                    throw ClientExceptionSupport.createNonFatalOrPassthrough(e.getCause());
                }
            }
        }
    }

    //----- Reconnection related internal API

    private void attemptConnection(String host, Integer port) {
        try {
            transport = ioContext.newTransport();
            LOG.trace("Attempting connection to remote {}:{}", host, port);
            transport.connect(host, port, new ClientTransportListener(engine));
        } catch (Throwable error) {
            engine.engineFailed(ClientExceptionSupport.createOrPassthroughFatal(error));
        }
    }

    private void scheduleConnect(String host, int port) {
        LOG.trace("Initial connect attempt will be performed immediately");
        executor.execute(() -> attemptConnection(host, port));
    }
}

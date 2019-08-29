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
package org.messaginghub.amqperative.client;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.apache.qpid.proton4j.engine.impl.ProtonEngine;
import org.apache.qpid.proton4j.engine.impl.ProtonEngineFactory;
import org.messaginghub.amqperative.Client;
import org.messaginghub.amqperative.Connection;
import org.messaginghub.amqperative.Message;
import org.messaginghub.amqperative.Receiver;
import org.messaginghub.amqperative.ReceiverOptions;
import org.messaginghub.amqperative.Sender;
import org.messaginghub.amqperative.SenderOptions;
import org.messaginghub.amqperative.Session;
import org.messaginghub.amqperative.SessionOptions;
import org.messaginghub.amqperative.Tracker;
import org.messaginghub.amqperative.client.exceptions.ClientConnectionRemotelyClosedException;
import org.messaginghub.amqperative.client.exceptions.ClientExceptionSupport;
import org.messaginghub.amqperative.client.exceptions.ClientIOException;
import org.messaginghub.amqperative.futures.AsyncResult;
import org.messaginghub.amqperative.futures.ClientFuture;
import org.messaginghub.amqperative.futures.ClientFutureFactory;
import org.messaginghub.amqperative.transport.Transport;
import org.messaginghub.amqperative.transport.impl.TcpTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;

/**
 * A {@link Connection} implementation that uses the Proton engine for AMQP protocol support.
 */
public class ClientConnection implements Connection {

    private static final Logger LOG = LoggerFactory.getLogger(ClientConnection.class);

    private static final long INFINITE = -1;
    private static final AtomicInteger CONNECTION_SEQUENCE = new AtomicInteger();

    private static final AtomicIntegerFieldUpdater<ClientConnection> CLOSED_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(ClientConnection.class, "closed");
    private static final AtomicReferenceFieldUpdater<ClientConnection, ClientException> FAILURE_CAUSE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(ClientConnection.class, ClientException.class, "failureCause");

    private final ClientInstance client;
    private final ClientConnectionOptions options;
    private final ClientFutureFactory futureFactoy;

    private final Map<ClientFuture<?>, ClientFuture<?>> requests = new ConcurrentHashMap<>();
    private final ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
    private org.apache.qpid.proton4j.engine.Connection protonConnection;
    private ClientSession connectionSession;
    private ClientSender connectionSender;
    private Transport transport;

    private ClientFuture<Connection> openFuture;
    private ClientFuture<Connection> closeFuture;
    private volatile int closed;
    private volatile int sessionCounter;
    private volatile ClientException failureCause;
    private final String connectionId;

    private ScheduledExecutorService executor;

    /**
     * Create a connection and define the initial configuration used to manage the
     * connection to the remote.
     *
     * @param client
     *      the {@link Client} that this connection resides within.
     * @param options
     *      the connection options that configure this {@link Connection} instance.
     */
    public ClientConnection(ClientInstance client, ClientConnectionOptions options) {
        this.client = client;
        this.options = options;
        this.connectionId = client.nextConnectionId();
        this.futureFactoy = ClientFutureFactory.create(options.getFutureType());

        ThreadFactory transportThreadFactory = new ClientThreadFactory(
            "ProtonConnection :(" + CONNECTION_SEQUENCE.incrementAndGet()
                          + "):[" + options.getHostname() + ":" + options.getPort() + "]", true);
        transport = new TcpTransport(
            new ClientTransportListener(this), options.getRemoteURI(), options.getTransport(), false);

        transport.setThreadFactory(transportThreadFactory);

        openFuture = futureFactoy.createFuture();
        closeFuture = futureFactoy.createFuture();
    }

    @Override
    public Client getClient() {
        return client;
    }

    @Override
    public Future<Connection> openFuture() {
        return openFuture;
    }

    @Override
    public Future<Connection> close() {
        if (CLOSED_UPDATER.compareAndSet(this, 0, 1) && !openFuture.isFailed()) {
            executor.execute(() -> {
                try {
                    protonConnection.close();
                } catch (Throwable ignored) {
                    LOG.trace("Error on attempt to close proton connection was ignored");
                    try {
                        transport.close();
                    } catch (IOException ignore) {}
                    closeFuture.complete(ClientConnection.this);
                }
            });
        }
        return closeFuture;
    }

    @Override
    public Session defaultSession() throws ClientException {
        return lazyCreateConnectionSession();
    }

    @Override
    public Session openSession() throws ClientException {
        return openSession(options.getDefaultSessionOptions());
    }

    @Override
    public Session openSession(SessionOptions sessionOptions) throws ClientException {
        checkClosed();
        final ClientFuture<Session> createSession = getFutureFactory().createFuture();
        final SessionOptions sessionOpts = sessionOptions == null ? options.getDefaultSessionOptions() : sessionOptions;

        executor.execute(() -> {
            ClientSession session = new ClientSession(sessionOpts, ClientConnection.this, protonConnection.session());
            createSession.complete(session.open());
        });

        return request(createSession, options.getRequestTimeout(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Receiver openReceiver(String address) throws ClientException {
        return openReceiver(address, options.getDefaultReceiverOptions());
    }

    @Override
    public Receiver openReceiver(String address, ReceiverOptions receiverOptions) throws ClientException {
        checkClosed();
        Objects.requireNonNull(address, "Cannot create a receiver with a null address");
        final ClientFuture<Receiver> createReceiver = getFutureFactory().createFuture();
        final ReceiverOptions receiverOpts = receiverOptions == null ? options.getDefaultReceiverOptions() : receiverOptions;

        executor.execute(() -> {
            try {
                checkClosed();
                createReceiver.complete(lazyCreateConnectionSession().internalCreateReceiver(address, receiverOpts).open());
            } catch (Throwable error) {
                createReceiver.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return request(createReceiver, options.getRequestTimeout(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Sender defaultSender() throws ClientException {
        return lazyCreateConnectionSender();
    }

    @Override
    public Sender openSender(String address) throws ClientException {
        return openSender(address, options.getDefaultSenderOptions());
    }

    @Override
    public Sender openSender(String address, SenderOptions senderOptions) throws ClientException {
        checkClosed();
        Objects.requireNonNull(address, "Cannot create a sender with a null address");
        final ClientFuture<Sender> createSender = getFutureFactory().createFuture();
        final SenderOptions senderOpts = senderOptions == null ? options.getDefaultSenderOptions() : senderOptions;

        executor.execute(() -> {
            try {
                checkClosed();
                createSender.complete(lazyCreateConnectionSession().internalCreateSender(address, senderOpts).open());
            } catch (Throwable error) {
                createSender.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return request(createSender, options.getRequestTimeout(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Sender openAnonymousSender() throws ClientException {
        return openAnonymousSender(new SenderOptions().setDynamic(true));
    }

    @Override
    public Sender openAnonymousSender(SenderOptions senderOptions) throws ClientException {
        checkClosed();
        final ClientFuture<Sender> createSender = getFutureFactory().createFuture();
        final SenderOptions senderOpts = senderOptions == null ? options.getDefaultSenderOptions() : senderOptions;

        executor.execute(() -> {
            try {
                checkClosed();
                createSender.complete(lazyCreateConnectionSession().internalCreateSender(null, senderOpts).open());
            } catch (Throwable error) {
                createSender.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return request(createSender, options.getRequestTimeout(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Tracker send(Message<?> message) throws ClientException {
        checkClosed();
        Objects.requireNonNull(message, "Cannot send a null message");
        final ClientFuture<Tracker> result = getFutureFactory().createFuture();

        executor.execute(() -> {
            try {
                checkClosed();
                result.complete(lazyCreateConnectionSender().send(message));
            } catch (Throwable error) {
                result.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return request(result, options.getSendTimeout(), TimeUnit.MILLISECONDS);
    }

    //----- Internal API

    String getId() {
        return connectionId;
    }

    ClientConnection connect() {
        try {
            executor = transport.connect(() -> {
                protonConnection = engine.start();

                protonConnection.openEventHandler(result -> {
                    openFuture.complete(this);
                });

                protonConnection.closeEventHandler(result -> {
                    // TODO - On remote close we need to ensure that sessions and their resources
                    //        all reflect the fact that they are now closed.  Also there is not a
                    //        way currently to reflect the fact that a remote closed happened in
                    //        the existing imperative client API.

                    // Close should be idempotent so we can just respond here with a close in case
                    // of remotely closed connection.  We should set error state from remote though
                    // so client can see it.
                    try {
                        protonConnection.close();
                    } catch (Throwable ignored) {
                        LOG.trace("Error on attempt to close proton connection was ignored");
                    }
                    try {
                        transport.close();
                    } catch (IOException ignore) {}
                    if (result.getRemoteCondition() != null) {
                        // TODO - Convert remote error to client exception
                        ClientException ex = new ClientConnectionRemotelyClosedException("Remote closed with error");
                        FAILURE_CAUSE_UPDATER.compareAndSet(this, null, ex);
                    }
                    CLOSED_UPDATER.lazySet(this, 1);
                    closeFuture.complete(this);
                });

                engine.outputHandler((toWrite) -> {
                    try {
                        ByteBuf outbound = transport.allocateSendBuffer(toWrite.getReadableBytes());
                        outbound.writeBytes(toWrite.toByteBuffer());

                        transport.write(outbound);
                        transport.flush();
                    } catch (IOException e) {
                        LOG.warn("Error while writing engine output to transport:", e);
                        e.printStackTrace();
                    }
                });
            }, null);
        } catch (Throwable e) {
            openFuture.failed(ClientExceptionSupport.createNonFatalOrPassthrough(e));
            try {
                transport.close();
            } catch (Throwable t) {
                LOG.trace("close of transport reported error", t);
            }

            CLOSED_UPDATER.set(this, 1);
            FAILURE_CAUSE_UPDATER.compareAndSet(this, null, ClientExceptionSupport.createOrPassthroughFatal(e));
        }

        return this;
    }

    ClientConnection open() {
        checkClosed();
        executor.execute(() -> {
            if (client.getContainerId() != null) {
                protonConnection.setContainerId(client.getContainerId());
            }
            options.configureConnection(protonConnection).open();
        });

        return this;
    }

    boolean isClosed() {
        return closed > 0;
    }

    ScheduledExecutorService getScheduler() {
        return executor;
    }

    ProtonEngine getEngine() {
        return engine;
    }

    ClientFutureFactory getFutureFactory() {
        return futureFactoy;
    }

    ClientException getFailureCause() {
        return failureCause;
    }

    String nextSessionId() {
        return getId() + ":" + (++sessionCounter);
    }

    void handleClientIOException(ClientIOException error) {
        FAILURE_CAUSE_UPDATER.compareAndSet(this, null, error);
        try {
            executor.execute(() -> {
                protonConnection.close();
                try {
                    transport.close();
                } catch (IOException ignored) {
                }
                // Signal any waiters that the operation is done due to error.
                openFuture.failed(error);
                closeFuture.complete(ClientConnection.this);
            });
        } catch (Throwable ingored) {
            LOG.trace("Ignoring error while closing down from client internal exception: ", ingored);
        }
    }

    <T> T request(ClientFuture<T> request, long timeout, TimeUnit units) throws ClientException {
        requests.put(request, request);

        try {
            if (timeout > 0) {
                return request.get(timeout, units);
            } else {
                return request.get();
            }
        } catch (Throwable error) {
            throw ClientExceptionSupport.createNonFatalOrPassthrough(error);
        } finally {
            requests.remove(request);
        }
    }

    ScheduledFuture<?> scheduleRequestTimeout(final AsyncResult<?> request, long timeout, final ClientException error) {
        if (timeout != INFINITE) {
            return executor.schedule(() -> request.failed(error), timeout, TimeUnit.MILLISECONDS);
        } else {
            return null;
        }
    }

    //----- Private implementation

    private ClientSession lazyCreateConnectionSession() {
        if (connectionSession == null) {
            connectionSession = new ClientSession(options.getDefaultSessionOptions(), this, protonConnection.session()).open();
        }

        return connectionSession;
    }

    private ClientSender lazyCreateConnectionSender() {
        if (connectionSender == null) {
            // TODO - Ensure this creates an anonymous sender
            // TODO - What if remote doesn't support anonymous?
            connectionSender = lazyCreateConnectionSession().internalCreateSender(null, options.getDefaultSenderOptions()).open();
        }

        return connectionSender;
    }

    protected void checkClosed() throws IllegalStateException {
        if (CLOSED_UPDATER.get(this) > 0) {
            throw new IllegalStateException("The Connection is closed");
        }
    }
}

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
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
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
import org.messaginghub.amqperative.client.exceptions.ClientExceptionSupport;
import org.messaginghub.amqperative.client.exceptions.ClientIOException;
import org.messaginghub.amqperative.futures.ClientFuture;
import org.messaginghub.amqperative.futures.ClientFutureFactory;
import org.messaginghub.amqperative.transport.Transport;
import org.messaginghub.amqperative.transport.TransportOptions;
import org.messaginghub.amqperative.transport.impl.TcpTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;

/**
 * A {@link Connection} implementation that uses the Proton engine for AMQP protocol support.
 */
public class ClientConnection implements Connection {

    private static final Logger LOG = LoggerFactory.getLogger(ClientConnection.class);

    private static final AtomicInteger CONNECTION_SEQUENCE = new AtomicInteger();

    private static final AtomicLongFieldUpdater<ClientConnection> CLOSE_STATE_UPDATER =
        AtomicLongFieldUpdater.newUpdater(ClientConnection.class, "closeState");
    private static final AtomicReferenceFieldUpdater<ClientConnection, ClientException> FAILURE_CAUSE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(ClientConnection.class, ClientException.class, "failureCause");

    private final ClientInstance container;
    private final ClientConnectionOptions options;
    private final ClientFutureFactory futureFactoy;

    private final ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
    private org.apache.qpid.proton4j.engine.Connection protonConnection;
    private ClientSession connectionSession;
    private ClientSender connectionSender;
    private Transport transport;

    private ClientFuture<Connection> openFuture;
    private ClientFuture<Connection> closeFuture;
    private final AtomicBoolean remoteOpened = new AtomicBoolean();
    private final AtomicBoolean remoteClosed = new AtomicBoolean();
    private volatile long closeState;
    private volatile ClientException failureCause;
    private String connectionId;

    private ScheduledExecutorService executor;

    /**
     * Create a connection and define the initial configuration used to manage the
     * connection to the remote.
     *
     * @param container
     *      the {@link Client} that this connection resides within.
     * @param options
     *      the connection options that configure this {@link Connection} instance.
     */
    public ClientConnection(ClientInstance container, ClientConnectionOptions options) {
        this.container = container;
        this.options = options;
        this.futureFactoy = ClientFutureFactory.create(options.getFutureType());

        ThreadFactory transportThreadFactory = new ClientThreadFactory(
            "ProtonConnection :(" + CONNECTION_SEQUENCE.incrementAndGet()
                          + "):[" + options.getHostname() + ":" + options.getPort() + "]", true);
        transport = new TcpTransport(
            new ClientTransportListener(this), options.getRemoteURI(), new TransportOptions(), false);

        transport.setThreadFactory(transportThreadFactory);

        openFuture = futureFactoy.createFuture();
        closeFuture = futureFactoy.createFuture();
        connectionId = UUID.randomUUID().toString();  // TODO - Sequence number of connection
    }

    @Override
    public Future<Connection> openFuture() {
        return openFuture;
    }

    @Override
    public Future<Connection> close() {
        if (CLOSE_STATE_UPDATER.compareAndSet(this, 0, 1) && !openFuture.isFailed()) {
            executor.execute(() -> {
                protonConnection.close();
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
        return openSession(new ClientSessionOptions());
    }

    @Override
    public Session openSession(SessionOptions options) throws ClientException {
        checkClosed();
        ClientFuture<Session> createSession = getFutureFactory().createFuture();

        executor.execute(() -> {
            // TODO - This relies on protonConnection having been created by an open already
            //        if connect is not a requirement of create connection then we need a
            //        lazy connect style call that ensures the engine is created and started
            //        so that the proton connection exists.
            ClientSession session = new ClientSession(
                new ClientSessionOptions(), ClientConnection.this, protonConnection.session());
            createSession.complete(session.open());
        });

        return request(createSession, options.getRequestTimeout(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Receiver openReceiver(String address) throws ClientException {
        Objects.requireNonNull(address, "Cannot create a sender with a null address");
        return openReceiver(address, new ClientReceiverOptions());
    }

    @Override
    public Receiver openReceiver(String address, ReceiverOptions receiverOptions) throws ClientException {
        checkClosed();
        ClientFuture<Receiver> createReceiver = getFutureFactory().createFuture();

        executor.execute(() -> {
            try {
                checkClosed();
                createReceiver.complete(lazyCreateConnectionSession().internalCreateReceiver(address, receiverOptions).open());
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
        return openSender(address, new ClientSenderOptions());
    }

    @Override
    public Sender openSender(String address, SenderOptions senderOptions) throws ClientException {
        checkClosed();
        ClientFuture<Sender> createSender = getFutureFactory().createFuture();

        executor.execute(() -> {
            try {
                checkClosed();
                createSender.complete(lazyCreateConnectionSession().internalCreateSender(address, senderOptions).open());
            } catch (Throwable error) {
                createSender.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return request(createSender, options.getRequestTimeout(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Sender openAnonymousSender() throws ClientException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Sender openAnonymousSender(SenderOptions senderOptions) throws ClientException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Tracker send(Message<?> message) throws ClientException {
        checkClosed();
        ClientFuture<Tracker> result = getFutureFactory().createFuture();

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

                protonConnection.openEventHandler((result) -> {
                    remoteOpened.set(true);
                    openFuture.complete(this);
                });

                protonConnection.closeEventHandler(result -> {
                    remoteClosed.set(true);
                    CLOSE_STATE_UPDATER.lazySet(this, 1);
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

            // TODO - Set closed and error state due to failed connect
        }

        return this;
    }

    ClientConnection open() {
        checkClosed();
        executor.execute(() -> {
            if (container.getContainerId() != null) {
                protonConnection.setContainerId(container.getContainerId());
            }
            options.configureConnection(protonConnection).open();
        });

        return this;
    }

    boolean isClosed() {
        return closeState > 0;
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

    void handleClientException(ClientIOException error) {
        // TODO - Implement handling of critical exception from IO etc.
        //        maybe rename to handleFatalException or something
        FAILURE_CAUSE_UPDATER.compareAndSet(this, null, error);
        try {
            executor.execute(() -> {
                protonConnection.close();
                // Signal any waiters that the operation is done due to error.
                openFuture.failed(error);
                closeFuture.complete(ClientConnection.this);
            });
        } catch (Throwable ingored) {
            LOG.trace("Ignoring error while closing down from client internal exception: ", ingored);
        }
    }

    <T> T request(ClientFuture<T> request, long timeout, TimeUnit units) throws ClientException {
        try {
            if (timeout > 0) {
                return request.get(timeout, units);
            } else {
                return request.get();
            }
        } catch (Throwable error) {
            throw ClientExceptionSupport.createNonFatalOrPassthrough(error);
        } finally {
            // TODO - Remove request from request map
        }
    }

    //----- Private implementation

    private ClientSession lazyCreateConnectionSession() {
        if (connectionSession == null) {
            connectionSession = new ClientSession(
                new ClientSessionOptions(), this, protonConnection.session());
            connectionSession.open();
        }

        return connectionSession;
    }

    private ClientSender lazyCreateConnectionSender() {
        if (connectionSender == null) {
            // TODO - Ensure this creates an anonymous sender
            // TODO - What if remote doesn't support anonymous?
            connectionSender = lazyCreateConnectionSession().internalCreateSender(null, new ClientSenderOptions());
            connectionSender.open();
        }

        return connectionSender;
    }

    protected void checkClosed() throws IllegalStateException {
        if (CLOSE_STATE_UPDATER.get(this) > 0) {
            throw new IllegalStateException("The Connection is closed");
        }
    }
}

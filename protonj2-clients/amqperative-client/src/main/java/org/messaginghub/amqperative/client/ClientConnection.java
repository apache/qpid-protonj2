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
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.proton4j.engine.impl.ProtonEngine;
import org.apache.qpid.proton4j.engine.impl.ProtonEngineFactory;
import org.messaginghub.amqperative.Connection;
import org.messaginghub.amqperative.Container;
import org.messaginghub.amqperative.Receiver;
import org.messaginghub.amqperative.ReceiverOptions;
import org.messaginghub.amqperative.Sender;
import org.messaginghub.amqperative.SenderOptions;
import org.messaginghub.amqperative.Session;
import org.messaginghub.amqperative.SessionOptions;
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

    private final ClientContainer container;
    private final ClientConnectionOptions options;
    private final ClientFutureFactory futureFactoy;

    private final ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
    private org.apache.qpid.proton4j.engine.Connection protonConnection;
    private ClientSession connectionSession;
    private Transport transport;

    private ClientFuture<Connection> openFuture;
    private ClientFuture<Connection> closeFuture;
    private final AtomicBoolean remoteOpened = new AtomicBoolean();
    private final AtomicBoolean remoteClosed = new AtomicBoolean();
    private volatile long closeState;
    private final AtomicReference<Throwable> failureCause = new AtomicReference<>();

    private ScheduledExecutorService executor;

    /**
     * Create a connection and define the initial configuration used to manage the
     * connection to the remote.
     *
     * @param container
     *      the {@link Container} that this connection resides within.
     * @param options
     *      the connection options that configure this {@link Connection} instance.
     */
    public ClientConnection(ClientContainer container, ClientConnectionOptions options) {
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
    }

    @Override
    public Future<Connection> openFuture() {
        // TODO: perhaps create a custom 'future' type?
        // Our tasks generally aren't going to be user-cancellable for example. The current impl uses CompletableFuture, which will say cancelled without any knowledge of the task at all. Could have knock on problems.
        // CompletionStage would give the stage handling and all the composition/execution bloat...but doesnt give a direct 'get result' style method itself....though does have 'toCompletableFuture' (but back to square one on cancel).
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
    public Session createSession() throws ClientException {
        return createSession(new ClientSessionOptions());
    }

    @Override
    public Session createSession(SessionOptions options) throws ClientException {
        ClientFuture<Session> result = getFutureFactory().createFuture();

        executor.execute(() -> {
            ClientSession session = new ClientSession(
                new ClientSessionOptions(), ClientConnection.this, protonConnection.session());

            // TODO - This relies on protonConnection having been created by an open already

            result.complete(session.open());
        });

        try {
            // TODO - Timeouts ?
            return result.get();
        } catch (Throwable e) {
            throw ClientExceptionSupport.createNonFatalOrPassthrough(e);
        }
    }

    @Override
    public Receiver createReceiver(String address) throws ClientException {
        return createReceiver(address, new ClientReceiverOptions());
    }

    @Override
    public Receiver createReceiver(String address, ReceiverOptions receiverOptions) throws ClientException {
        // TODO: await connection/session opening? Intertwine their open-futures?

        return connectionSession.createReceiver(address, receiverOptions);
    }

    @Override
    public Sender createSender(String address) throws ClientException {
        return createSender(address, new ClientSenderOptions());
    }

    @Override
    public Sender createSender(String address, SenderOptions senderOptions) throws ClientException {
        return connectionSession.createSender(address, senderOptions);
    }

    //----- Internal API

    ClientConnection connect() {
        try {
            //TODO: have the connect itself be async?
            executor = transport.connect(() -> {
                protonConnection = engine.start();

                protonConnection.openEventHandler((result) -> {
                    remoteOpened.set(true);

                    // TODO - Lazy create connection session for sender and receiver create
                    // Creates the Connection Session used for connection created senders and receivers.
                    // TODO - Open currently is done on connect so this is safe from NPE when doing
                    //        open -> begin -> attach things but could go horribly wrong later
                    connectionSession = new ClientSession(
                        new ClientSessionOptions(), ClientConnection.this, protonConnection.session()).open();

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

            return this;
        }

        open(); // TODO - Separate open from connect ?

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

    void handleClientException(ClientIOException createOrPassthroughFatal) {
        // TODO - Implement handling of critical exception from IO etc.
        //        maybe rename to handleFatalException or something
    }

    //----- Private implementation

    protected void checkClosed() throws IllegalStateException {
        if (CLOSE_STATE_UPDATER.get(this) > 0) {
            throw new IllegalStateException("The Connection is closed");
        }
    }

    private void open() {
        executor.execute(() -> {
            if (container.getContainerId() != null) {
                protonConnection.setContainerId(container.getContainerId());
            }
            protonConnection.open();
        });
    }
}

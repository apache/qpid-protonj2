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
package org.messaginghub.amqperative.impl;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.qpid.proton4j.buffer.ProtonByteBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.engine.exceptions.EngineStateException;
import org.apache.qpid.proton4j.engine.impl.ProtonEngine;
import org.apache.qpid.proton4j.engine.impl.ProtonEngineFactory;
import org.messaginghub.amqperative.Connection;
import org.messaginghub.amqperative.Receiver;
import org.messaginghub.amqperative.ReceiverOptions;
import org.messaginghub.amqperative.Sender;
import org.messaginghub.amqperative.SenderOptions;
import org.messaginghub.amqperative.transport.Transport;
import org.messaginghub.amqperative.transport.TransportListener;
import org.messaginghub.amqperative.transport.TransportOptions;
import org.messaginghub.amqperative.transport.impl.TcpTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;

/**
 * A {@link Connection} implementation that uses the Proton engine for AMQP protocol support.
 */
public class ProtonConnection implements Connection {

    private static final Logger LOG = LoggerFactory.getLogger(ProtonConnection.class);

    private static final AtomicInteger CONNECTION_SEQUENCE = new AtomicInteger();

    private final ProtonConnectionOptions options;

    private final ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
    private org.apache.qpid.proton4j.engine.Connection protonConnection;
    private Transport transport;

    private CompletableFuture<Connection> openFuture = new CompletableFuture<Connection>();
    private CompletableFuture<Connection> closeFuture = new CompletableFuture<Connection>();
    private AtomicBoolean remoteOpened = new AtomicBoolean();
    private AtomicBoolean remoteClosed = new AtomicBoolean();

    private ScheduledExecutorService executor;

    /**
     * Create a connection and define the initial configuration used to manage the
     * connection to the remote.
     *
     * @param options
     *      the connection options that configure this {@link Connection} instance.
     */
    public ProtonConnection(ProtonConnectionOptions options) {
        this.options = options;

        ThreadFactory transportThreadFactory = new ProtonConnectionThreadFactory(
            "ConnectionImpl :(" + CONNECTION_SEQUENCE.incrementAndGet()
                          + "):[" + options.getHostname() + ":" + options.getPort() + "]", true);
        transport = new TcpTransport(
            new TransportHandler(engine), options.getRemoteURI(), new TransportOptions(), false);

        transport.setThreadFactory(transportThreadFactory);
    }

    @Override
    public Future<Connection> openFuture() {
        return openFuture;  // TODO
    }

    @Override
    public Future<Connection> close() {
        if (!openFuture.isCompletedExceptionally()) {
            executor.schedule(() -> {
                protonConnection.close();
            }, 1, TimeUnit.SECONDS);//TODO: remove artificial delay, use execute
        }
        return closeFuture; // TODO
    }

    @Override
    public Receiver createReceiver(String address) {
        return createReceiver(address, new ProtonReceiverOptions());
    }

    @Override
    public Receiver createReceiver(String address, ReceiverOptions receiverOptions) {
        return new ProtonReceiver(receiverOptions);
    }

    @Override
    public Sender createSender(String address) {
        return createSender(address, new ProtonSenderOptions());
    }

    @Override
    public Sender createSender(String address, SenderOptions senderOptions) {
        return new ProtonSender(senderOptions);
    }

    //----- Internal API

    void connect() {
        try {
            //TODO: have the connect itself be async?
            executor = transport.connect(() -> {
                protonConnection = engine.start();

                protonConnection.openEventHandler((result) -> {
                    remoteOpened.set(true);
                    openFuture.complete(this);
                });

                protonConnection.closeEventHandler(result -> {
                    remoteClosed.set(true);
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
            openFuture.completeExceptionally(e);
            try {
                transport.close();
            } catch (Throwable t) {
                LOG.trace("close of transport reported error", t);
            }

            return;
        }

        open(); // TODO - Separate open from connect ?
    }

    //----- Private implementation

    private void open() {
        executor.schedule(() -> {
            protonConnection.open();
        }, 1, TimeUnit.SECONDS);//TODO: remove artificial delay, use execute
    }

    private static class TransportHandler implements TransportListener {
        private ProtonEngine engine;

        public TransportHandler(ProtonEngine engine) {
            this.engine = engine;
        }

        private ProtonByteBufferAllocator allocator = ProtonByteBufferAllocator.DEFAULT;

        @Override
        public void onData(ByteBuf incoming) {
            int readable = incoming.readableBytes();
            ProtonByteBuffer buffer = allocator.allocate(readable, readable);

            // Copy for now until the wrapper implementation is done.
            buffer.writeBytes(incoming.nioBuffer());

            try {
                engine.ingest(buffer);
            } catch (EngineStateException e) {
                LOG.warn("Engine ingest threw error: ", e);
            }
        }

        @Override
        public void onTransportClosed() {
            LOG.warn("Transport has closed");
        }

        @Override
        public void onTransportError(Throwable cause) {
            LOG.warn("Transport has reported an error:", cause);
        }
    }
}

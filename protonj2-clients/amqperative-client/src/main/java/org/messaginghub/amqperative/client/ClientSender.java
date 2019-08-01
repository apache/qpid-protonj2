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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.qpid.proton4j.amqp.transport.DeliveryState;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.engine.OutgoingDelivery;
import org.messaginghub.amqperative.Message;
import org.messaginghub.amqperative.Sender;
import org.messaginghub.amqperative.SenderOptions;
import org.messaginghub.amqperative.Tracker;
import org.messaginghub.amqperative.client.exceptions.ClientExceptionSupport;
import org.messaginghub.amqperative.futures.ClientFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Proton based AMQP Sender
 */
public class ClientSender implements Sender {

    private static final Logger LOG = LoggerFactory.getLogger(ClientSender.class);

    private final ClientFuture<Sender> openFuture;
    private final ClientFuture<Sender> closeFuture;

    private final ClientSenderOptions options;
    private final ClientSession session;
    private final org.apache.qpid.proton4j.engine.Sender sender;
    private final ScheduledExecutorService executor;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicReference<Throwable> failureCause = new AtomicReference<>();

    public ClientSender(SenderOptions options, ClientSession session, org.apache.qpid.proton4j.engine.Sender sender) {
        this.options = new ClientSenderOptions(options);
        this.session = session;
        this.sender = sender;
        this.executor = session.getScheduler();
        this.openFuture = session.getFutureFactory().createFuture();
        this.closeFuture = session.getFutureFactory().createFuture();
    }

    @Override
    public Future<Sender> openFuture() {
        return openFuture;
    }

    @Override
    public Future<Sender> close() {
        if (closed.compareAndSet(false, true) && !openFuture.isFailed()) {
            executor.execute(() -> {
                sender.close();
            });
        }
        return closeFuture;
    }

    @Override
    public Future<Sender> detach() {
        if (closed.compareAndSet(false, true) && !openFuture.isFailed()) {
            executor.execute(() -> {
                sender.detach();
            });
        }
        return closeFuture;
    }

    @Override
    public Tracker send(Message<?> message) throws ClientException {
        checkClosed();
        ClientFuture<Tracker> operation = session.getFutureFactory().createFuture();

        executor.execute(() -> {
            //TODO: block for credit
            //TODO: check sender.isSendable();

            ClientMessage<?> msg = (ClientMessage<?>) message;

            ProtonBuffer buffer = ClientMessageSupport.encodeMessage(msg);
            OutgoingDelivery delivery = sender.next();
            delivery.setTag(new byte[] {0});
            delivery.writeBytes(buffer);

            operation.complete(new ClientTracker(this, delivery));
        });

        try {
            // TODO - Timeouts ?
            return operation.get();
        } catch (Throwable e) {
            throw ClientExceptionSupport.createNonFatalOrPassthrough(e);
        }
    }

    @Override
    public Tracker trySend(Message<?> message, Consumer<Tracker> onUpdated) throws IllegalStateException {
        checkClosed();
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Tracker send(Message<?> message, Consumer<Tracker> onUpdated) {
        checkClosed();
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Tracker send(Message<?> message, Consumer<Tracker> onUpdated, ExecutorService executor) {
        checkClosed();
        // TODO Auto-generated method stub
        return null;
    }

    //----- Internal API

    void disposition(OutgoingDelivery delivery, DeliveryState state, boolean settled) {
        checkClosed();
        executor.execute(() -> {
            delivery.disposition(state, settled);
        });
    }

    ClientSender open() {
        executor.execute(() -> {
            sender.openHandler(result -> {
                if (result.succeeded()) {
                    openFuture.complete(this);
                    LOG.trace("Sender opened successfully");
                } else {
                    openFuture.failed(ClientExceptionSupport.createNonFatalOrPassthrough(result.error()));
                    LOG.error("Sender failed to open: ", result.error());
                }
            });

            sender.closeHandler(result -> {
                closed.set(true);
                closeFuture.complete(this);
            });

            sender.detachHandler(result -> {
                // TODO
            });

            sender.deliveryUpdatedEventHandler(delivery -> {
                // TODO
            });

            sender.sendableEventHandler(result -> {
                // TODO
            });

            options.configureSender(sender).open();
        });

        return this;
    }

    void setFailureCause(Throwable failureCause) {
        this.failureCause.set(failureCause);
    }

    Throwable getFailureCause() {
        if (failureCause.get() == null) {
            return session.getFailureCause();
        }

        return failureCause.get();
    }

    //----- Private implementation details

    private void checkClosed() throws IllegalStateException {
        if (closed.get()) {
            IllegalStateException error = null;

            if (getFailureCause() == null) {
                error = new IllegalStateException("The Sender is closed");
            } else {
                error = new IllegalStateException("The Sender was closed due to an unrecoverable error.");
                error.initCause(getFailureCause());
            }

            throw error;
        }
    }
}

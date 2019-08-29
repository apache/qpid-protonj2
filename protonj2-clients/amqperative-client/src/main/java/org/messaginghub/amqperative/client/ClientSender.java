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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.qpid.proton4j.amqp.transport.DeliveryState;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.engine.LinkCreditState;
import org.apache.qpid.proton4j.engine.LinkState;
import org.apache.qpid.proton4j.engine.OutgoingDelivery;
import org.messaginghub.amqperative.Client;
import org.messaginghub.amqperative.Message;
import org.messaginghub.amqperative.Sender;
import org.messaginghub.amqperative.SenderOptions;
import org.messaginghub.amqperative.Session;
import org.messaginghub.amqperative.Tracker;
import org.messaginghub.amqperative.client.exceptions.ClientSendTimedOutException;
import org.messaginghub.amqperative.futures.AsyncResult;
import org.messaginghub.amqperative.futures.ClientFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Proton based AMQP Sender
 */
public class ClientSender implements Sender {

    private static final Logger LOG = LoggerFactory.getLogger(ClientSender.class);

    private static final AtomicIntegerFieldUpdater<ClientSender> CLOSED_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(ClientSender.class, "closed");

    private final ClientFuture<Sender> openFuture;
    private final ClientFuture<Sender> closeFuture;

    private final Map<Object, InFlightSend> blocked = new LinkedHashMap<Object, InFlightSend>();
    private final ClientSenderOptions options;
    private final ClientSession session;
    private final org.apache.qpid.proton4j.engine.Sender protonSender;
    private final ScheduledExecutorService executor;
    private final AtomicReference<Throwable> failureCause = new AtomicReference<>();
    private final String senderId;
    private volatile int closed;
    private LinkCreditState drainingState;

    public ClientSender(SenderOptions options, ClientSession session, org.apache.qpid.proton4j.engine.Sender sender, String address) {
        this.options = new ClientSenderOptions(options);
        this.session = session;
        this.protonSender = sender;
        this.senderId = session.nextSenderId();
        this.executor = session.getScheduler();
        this.openFuture = session.getFutureFactory().createFuture();
        this.closeFuture = session.getFutureFactory().createFuture();

        this.options.configureSender(sender, address);
    }

    @Override
    public Client getClient() {
        return session.getClient();
    }

    @Override
    public Session getSession() {
        return session;
    }

    @Override
    public Future<Sender> openFuture() {
        return openFuture;
    }

    @Override
    public Future<Sender> close() {
        if (CLOSED_UPDATER.compareAndSet(this, 0, 1) && !openFuture.isFailed()) {
            executor.execute(() -> {
                protonSender.close();
            });
        }
        return closeFuture;
    }

    @Override
    public Future<Sender> detach() {
        if (CLOSED_UPDATER.compareAndSet(this, 0, 1) && !openFuture.isFailed()) {
            executor.execute(() -> {
                protonSender.detach();
            });
        }
        return closeFuture;
    }

    @Override
    public Tracker send(Message<?> message) throws ClientException {
        checkClosed();
        ClientFuture<Tracker> operation = session.getFutureFactory().createFuture();

        executor.execute(() -> {
            if (protonSender.isSendable()) {
                assumeSendableAndSend((ClientMessage<?>) message, operation);
            } else {
                final ScheduledFuture<?> sendTimeout;
                if (options.getSendTimeout() > 0) {
                    // TODO - Add variant that takes a builder of exceptions to reduce allocation overhead.
                    sendTimeout = session.scheduleRequestTimeout(
                        operation, options.getSendTimeout(), new ClientSendTimedOutException("Timed out waiting for send"));
                } else {
                    sendTimeout = null;
                }

                final InFlightSend send = new InFlightSend((ClientMessage<?>) message, operation, sendTimeout);
                blocked.put(send, send);
            }
        });

        return session.request(operation, options.getSendTimeout(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Tracker trySend(Message<?> message) throws ClientException {
        checkClosed();
        ClientFuture<Tracker> operation = session.getFutureFactory().createFuture();
        executor.execute(() -> {
            if (protonSender.isSendable()) {
                assumeSendableAndSend((ClientMessage<?>) message, operation);
            } else {
                operation.complete(null);
            }
        });

        return session.request(operation, options.getSendTimeout(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Tracker send(Message<?> message, Consumer<Tracker> onUpdated) {
        checkClosed();
        // TODO - So many questions
        return null;
    }

    @Override
    public Tracker send(Message<?> message, Consumer<Tracker> onUpdated, ExecutorService executor) {
        checkClosed();
        // TODO - Even more questions
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
            protonSender.openHandler(sender -> handleRemoteOpen(sender))
                        .closeHandler(sender -> handleRemoteCloseOrDetach(sender))
                        .detachHandler(sender -> handleRemoteCloseOrDetach(sender))
                        .deliveryUpdatedEventHandler(delivery -> handleDeliveryUpdated(delivery))
                        .drainRequestedEventHandler(linkState -> handleRemoteRequestedDrain(linkState))
                        .sendableEventHandler(sender -> handleRemoteNowSendable(sender))
                        .open();

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

    String getId() {
        return senderId;
    }

    boolean isClosed() {
        return closed > 0;
    }

    //----- Handlers for proton receiver events

    private void handleRemoteOpen(org.apache.qpid.proton4j.engine.Sender sender) {
        // Check for deferred close pending and hold completion if so
        if (sender.getRemoteTarget() != null) {
            openFuture.complete(this);
            LOG.trace("Sender opened successfully");
        } else {
            LOG.debug("Sender opened but remote signalled close is pending: ", sender);
        }
    }

    private void handleRemoteCloseOrDetach(org.apache.qpid.proton4j.engine.Sender sender) {
        CLOSED_UPDATER.lazySet(this, 1);

        // Close should be idempotent so we can just respond here with a close in case
        // of remotely closed sender.  We should set error state from remote though
        // so client can see it.
        try {
            if (protonSender.getRemoteState() == LinkState.CLOSED) {
                LOG.info("Sender link remotely closed: ", sender);
                protonSender.close();
            } else {
                LOG.info("Sender link remotely detached: ", sender);
                protonSender.detach();
            }
        } catch (Throwable ignored) {
            LOG.trace("Error while processing remote close event: ", ignored);
        }

        if (sender.getRemoteTarget() == null) {
            openFuture.failed(new ClientException("Link creation was refused"));
        } else {
            openFuture.complete(this);
        }
        closeFuture.complete(this);
    }

    private void handleRemoteNowSendable(org.apache.qpid.proton4j.engine.Sender sender) {
        if (!blocked.isEmpty()) {
            Iterator<InFlightSend> blockedSends = blocked.values().iterator();
            while (sender.isSendable() && blockedSends.hasNext()) {
                LOG.trace("Dispatching previously held send");
                InFlightSend held = blockedSends.next();
                try {
                    assumeSendableAndSend(held.message, held);
                } finally {
                    blockedSends.remove();
                }
            }
        }

        if (drainingState != null) {
            sender.drained(drainingState);
            drainingState = null;
        }
    }

    private void handleRemoteRequestedDrain(LinkCreditState linkState) {
        if (blocked.isEmpty()) {
            protonSender.drained(linkState);
        } else {
            drainingState = linkState;
        }
    }

    private void handleDeliveryUpdated(OutgoingDelivery delivery) {
        // TODO - Signal received etc
    }

    //----- Send Result Tracker used for send blocked on credit

    private class InFlightSend implements AsyncResult<Tracker> {

        private final ClientMessage<?> message;
        private final ClientFuture<Tracker> operation;
        private final ScheduledFuture<?> timeout;

        public InFlightSend(ClientMessage<?> message, ClientFuture<Tracker> operation, ScheduledFuture<?> timeout) {
            this.message = message;
            this.operation = operation;
            this.timeout = timeout;
        }

        @Override
        public void failed(ClientException result) {
            if (timeout != null) {
                timeout.cancel(true);
            }
            operation.failed(result);
        }

        @Override
        public void complete(Tracker result) {
            if (timeout != null) {
                timeout.cancel(true);
            }
            operation.complete(result);
        }

        @Override
        public boolean isComplete() {
            return operation.isDone();
        }
    }

    //----- Private implementation details

    private void assumeSendableAndSend(ClientMessage<?> message, AsyncResult<Tracker> request) {
        ProtonBuffer buffer = ClientMessageSupport.encodeMessage(message);
        OutgoingDelivery delivery = protonSender.next();
        delivery.setTag(new byte[] {0});
        delivery.writeBytes(buffer);

        request.complete(new ClientTracker(this, delivery));
    }

    private void checkClosed() throws IllegalStateException {
        if (isClosed()) {
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

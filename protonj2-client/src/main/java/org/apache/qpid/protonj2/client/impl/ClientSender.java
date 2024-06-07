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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.client.AdvancedMessage;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Sender;
import org.apache.qpid.protonj2.client.SenderOptions;
import org.apache.qpid.protonj2.client.Tracker;
import org.apache.qpid.protonj2.client.exceptions.ClientConnectionRemotelyClosedException;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientResourceRemotelyClosedException;
import org.apache.qpid.protonj2.client.exceptions.ClientSendTimedOutException;
import org.apache.qpid.protonj2.client.futures.ClientFuture;
import org.apache.qpid.protonj2.engine.OutgoingDelivery;
import org.apache.qpid.protonj2.types.transport.DeliveryState;
import org.apache.qpid.protonj2.types.transport.SenderSettleMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Proton based AMQP Sender
 */
public final class ClientSender extends ClientSenderLinkType<Sender> implements Sender {

    private static final Logger LOG = LoggerFactory.getLogger(ClientSender.class);

    private final Deque<ClientOutgoingEnvelope> blocked = new ArrayDeque<>();
    private final SenderOptions options;

    ClientSender(ClientSession session, SenderOptions options, String senderId, org.apache.qpid.protonj2.engine.Sender protonSender) {
        super(session, senderId, options, protonSender);

        this.options = new SenderOptions(options);
    }

    @Override
    public Tracker send(Message<?> message) throws ClientException {
        checkClosedOrFailed();
        return sendMessage(ClientMessageSupport.convertMessage(message), null, true);
    }

    @Override
    public Tracker send(Message<?> message, Map<String, Object> deliveryAnnotations) throws ClientException {
        checkClosedOrFailed();
        return sendMessage(ClientMessageSupport.convertMessage(message), deliveryAnnotations, true);
    }

    @Override
    public Tracker trySend(Message<?> message) throws ClientException {
        checkClosedOrFailed();
        return sendMessage(ClientMessageSupport.convertMessage(message), null, false);
    }

    @Override
    public Tracker trySend(Message<?> message, Map<String, Object> deliveryAnnotations) throws ClientException {
        checkClosedOrFailed();
        return sendMessage(ClientMessageSupport.convertMessage(message), deliveryAnnotations, false);
    }

    //----- Internal API

    SenderOptions options() {
        return this.options;
    }

    @Override
    void disposition(OutgoingDelivery delivery, DeliveryState state, boolean settled) throws ClientException {
        checkClosedOrFailed();
        executor.execute(() -> {
            delivery.disposition(state, settled);
        });
    }

    //----- Handlers for proton receiver events

    private void handleCreditStateUpdated(org.apache.qpid.protonj2.engine.Sender sender) {
        if (!blocked.isEmpty()) {
            while (sender.isSendable() && !blocked.isEmpty()) {
                ClientOutgoingEnvelope held = blocked.peek();
                if (held.delivery() == protonSender.current()) {
                    LOG.trace("Dispatching previously held send");
                    try {
                        // We don't currently allow a sender to define any outcome so we pass null for
                        // now, however a transaction context will apply its TransactionalState outcome
                        // and would wrap anything we passed in the future.
                        session.getTransactionContext().send(held, null, isSendingSettled());
                    } catch (Exception error) {
                        held.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
                    } finally {
                        blocked.poll();
                    }
                } else {
                    break;
                }
            }
        }

        if (sender.isDraining() && sender.current() == null && blocked.isEmpty()) {
            sender.drained();
        }
    }

    //----- Internal class implementation details

    @Override
    protected Sender self() {
        return this;
    }

    private void addToTailOfBlockedQueue(ClientOutgoingEnvelope send) {
        blocked.addLast(send);
        if (options.sendTimeout() > 0 && send.sendTimeout() == null) {
            send.sendTimeout(executor.schedule(() -> {
                blocked.remove(send);
                send.failed(send.createSendTimedOutException());
            }, options.sendTimeout(), TimeUnit.MILLISECONDS));
        }
    }

    private void addToHeadOfBlockedQueue(ClientOutgoingEnvelope send) {
        blocked.addFirst(send);
        if (options.sendTimeout() > 0 && send.sendTimeout() == null) {
            send.sendTimeout(executor.schedule(() -> {
                blocked.remove(send);
                send.failed(send.createSendTimedOutException());
            }, options.sendTimeout(), TimeUnit.MILLISECONDS));
        }
    }

    private Tracker sendMessage(AdvancedMessage<?> message, Map<String, Object> deliveryAnnotations, boolean waitForCredit) throws ClientException {
        final ClientFuture<Tracker> operation = session.getFutureFactory().createFuture();
        final ProtonBuffer buffer = message.encode(deliveryAnnotations, ProtonBufferAllocator.defaultAllocator());

        executor.execute(() -> {
            if (notClosedOrFailed(operation)) {
                try {
                    final ClientOutgoingEnvelope envelope = new ClientOutgoingEnvelope(this, message.messageFormat(), buffer, operation);

                    if (protonSender.isSendable() && protonSender.current() == null) {
                        session.getTransactionContext().send(envelope, null, protonSender.getSenderSettleMode() == SenderSettleMode.SETTLED);
                    } else if (waitForCredit) {
                        addToTailOfBlockedQueue(envelope);
                    } else {
                        operation.complete(null);
                    }
                } catch (Exception error) {
                    operation.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
                }
            }
        });

        return session.request(this, operation);
    }

    private Tracker createTracker(OutgoingDelivery delivery) {
        return new ClientTracker(this, delivery);
    }

    private Tracker createNoOpTracker() {
        return new ClientNoOpTracker(this);
    }

    @Override
    protected void linkSpecificLocalOpenHandler() {
        protonSender.creditStateUpdateHandler(this::handleCreditStateUpdated);
    }

    @Override
    protected void recreateLinkForReconnect() {
        protonSender.localCloseHandler(null);
        protonSender.localDetachHandler(null);
        protonSender.close();
        if (protonSender.hasUnsettled()) {
            failPendingUnsettledAndBlockedSends(
                new ClientConnectionRemotelyClosedException("Connection failed and send result is unknown"));
        }
        protonSender = ClientSenderBuilder.recreateSender(session, protonSender, options);
        protonSender.setLinkedResource(this);
    }

    @Override
    protected void linkSpecificCleanupHandler(ClientException failureCause) {
        if (failureCause != null) {
            failPendingUnsettledAndBlockedSends(failureCause);
        } else {
            failPendingUnsettledAndBlockedSends(new ClientResourceRemotelyClosedException("The sender link has closed"));
        }
    }

    private void failPendingUnsettledAndBlockedSends(ClientException cause) {
        // Cancel all settlement futures for in-flight sends passing an appropriate error to the future
        protonSender.unsettled().forEach((delivery) -> {
            try {
                delivery.getLinkedResource(ClientTrackable.class).settlementFuture().failed(cause);
            } catch (Exception e) {
            }
        });

        // Cancel all blocked sends passing an appropriate error to the future
        blocked.removeIf((held) -> {
            held.failed(cause);
            return true;
        });
    }

    @Override
    protected void linkSpecificLocalCloseHandler() {
        // Nothing needed for sender handling
    }

    @Override
    protected void linkSpecificRemoteOpenHandler() {
        // Nothing needed for sender handling
    }

    @Override
    protected void linkSpecificRemoteCloseHandler() {
        // Nothing needed for sender handling
    }

    //----- Internal envelope for deliveries to track potential partial sends etc.

    private final static class ClientOutgoingEnvelope implements ClientTransactionContext.Sendable {

        private final ProtonBuffer payload;
        private final ClientFuture<Tracker> request;
        private final ClientSender sender;
        private final int messageFormat;

        private Future<?> sendTimeout;
        private OutgoingDelivery delivery;

        /**
         * Create a new In-flight Send instance for a complete message send.  No further
         * sends can occur after the send completes.
         *
         * @param sender
         *      The {@link ClientSender} instance that is attempting to send this encoded message.
         * @param messageFormat
         *      The message format code to assign the send if this is the first delivery.
         * @param payload
         *      The payload that comprises this portion of the send.
         * @param request
         *      The requesting operation that initiated this send.
         */
        ClientOutgoingEnvelope(ClientSender sender, int messageFormat, ProtonBuffer payload, ClientFuture<Tracker> request) {
            this.messageFormat = messageFormat;
            this.payload = payload;
            this.request = request;
            this.sender = sender;
        }

        /**
         * @return the {@link Future} used to determine when the send should fail if no credit available to write.
         */
        public Future<?> sendTimeout() {
            return sendTimeout;
        }

        /**
         * Sets the {@link Future} which should be used when a send cannot be immediately performed.
         *
         * @param sendTimeout
         * 		The {@link Future} that will fail the send if not cancelled once it has been performed.
         */
        public void sendTimeout(Future<?> sendTimeout) {
            this.sendTimeout = sendTimeout;
        }

        public OutgoingDelivery delivery() {
            return delivery;
        }

        public ClientOutgoingEnvelope succeeded() {
            if (sendTimeout != null) {
                sendTimeout.cancel(true);
            }

            payload.close();

            request.complete(delivery.getLinkedResource());

            return this;
        }

        public ClientOutgoingEnvelope failed(ClientException exception) {
            if (sendTimeout != null) {
                sendTimeout.cancel(true);
            }

            if (delivery != null) {
                try {
                    delivery.abort();
                } catch (Exception ex) {
                    // Attempted abort could fail if offline so we ignore it.
                }
            }

            payload.close();

            request.failed(exception);

            return this;
        }

        @Override
        public void discard() {
            if (sendTimeout != null) {
                sendTimeout.cancel(true);
                sendTimeout = null;
            }

            try (payload) {
                if (delivery != null) {
                    ClientTracker tracker = delivery.getLinkedResource();
                    if (tracker != null) {
                        tracker.settlementFuture().complete(tracker);
                    }
                    request.complete(delivery.getLinkedResource());
                } else {
                    request.complete(sender.createNoOpTracker());
                }
            }
        }

        @Override
        public void send(DeliveryState state, boolean settled) {
            if (delivery == null) {
                delivery = sender.protonLink().next();
                delivery.setLinkedResource(sender.createTracker(delivery));
                delivery.setMessageFormat(messageFormat);
                delivery.disposition(state, settled);
            }

            boolean wasAutoFlushOn = sender.connection().autoFlushOff();
            try {
                delivery.streamBytes(payload, true);
                if (payload != null && payload.isReadable()) {
                    sender.addToHeadOfBlockedQueue(this);
                } else {
                    succeeded();
                }
            } finally {
                if (wasAutoFlushOn) {
                    sender.connection().flush();
                    sender.connection().autoFlushOn();
                }
            }
        }

        public ClientException createSendTimedOutException() {
            return new ClientSendTimedOutException("Timed out waiting for credit to send");
        }
    }
}

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
import org.apache.qpid.protonj2.client.AdvancedMessage;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.StreamSender;
import org.apache.qpid.protonj2.client.StreamSenderOptions;
import org.apache.qpid.protonj2.client.StreamTracker;
import org.apache.qpid.protonj2.client.exceptions.ClientConnectionRemotelyClosedException;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.client.exceptions.ClientResourceRemotelyClosedException;
import org.apache.qpid.protonj2.client.exceptions.ClientSendTimedOutException;
import org.apache.qpid.protonj2.client.futures.ClientFuture;
import org.apache.qpid.protonj2.client.futures.ClientSynchronization;
import org.apache.qpid.protonj2.engine.OutgoingDelivery;
import org.apache.qpid.protonj2.engine.util.StringUtils;
import org.apache.qpid.protonj2.types.messaging.DeliveryAnnotations;
import org.apache.qpid.protonj2.types.transport.DeliveryState;
import org.apache.qpid.protonj2.types.transport.SenderSettleMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client implementation of a {@link StreamSender}.
 */
public final class ClientStreamSender extends ClientSenderLinkType<StreamSender> implements StreamSender {

    private static final Logger LOG = LoggerFactory.getLogger(ClientStreamSender.class);

    private final StreamSenderOptions options;
    private final Deque<ClientOutgoingEnvelope> blocked = new ArrayDeque<>();

    ClientStreamSender(ClientSession session, StreamSenderOptions options, String senderId, org.apache.qpid.protonj2.engine.Sender protonSender) {
        super(session, senderId, options, protonSender);

        this.options = new StreamSenderOptions(options);
    }

    @Override
    public StreamTracker send(Message<?> message) throws ClientException {
        checkClosedOrFailed();
        return sendMessage(ClientMessageSupport.convertMessage(message), null, true);
    }

    @Override
    public StreamTracker send(Message<?> message, Map<String, Object> deliveryAnnotations) throws ClientException {
        checkClosedOrFailed();
        return sendMessage(ClientMessageSupport.convertMessage(message), null, true);
    }

    @Override
    public StreamTracker trySend(Message<?> message) throws ClientException {
        checkClosedOrFailed();
        return sendMessage(ClientMessageSupport.convertMessage(message), null, false);
    }

    @Override
    public StreamTracker trySend(Message<?> message, Map<String, Object> deliveryAnnotations) throws ClientException {
        checkClosedOrFailed();
        return sendMessage(ClientMessageSupport.convertMessage(message), null, false);
    }

    @Override
    public ClientStreamSenderMessage beginMessage() throws ClientException {
        return beginMessage(null);
    }

    @Override
    public ClientStreamSenderMessage beginMessage(Map<String, Object> deliveryAnnotations) throws ClientException {
        checkClosedOrFailed();
        final ClientFuture<ClientStreamSenderMessage> request = session.getFutureFactory().createFuture();
        final DeliveryAnnotations annotations;

        if (deliveryAnnotations != null) {
            annotations = new DeliveryAnnotations(StringUtils.toSymbolKeyedMap(deliveryAnnotations));
        } else {
            annotations = null;
        }

        executor.execute(() -> {
            if (protonSender.current() != null) {
                request.failed(new ClientIllegalStateException(
                    "Cannot initiate a new streaming send until the previous one is complete"));
            } else {
                // Grab the next delivery and hold for stream writes, no other sends
                // can occur while we hold the delivery.
                final OutgoingDelivery streamDelivery = protonSender.next();
                final ClientStreamTracker streamTracker = createTracker(streamDelivery);

                streamDelivery.setLinkedResource(streamTracker);

                request.complete(new ClientStreamSenderMessage(this, streamTracker, annotations));
            }
        });

        return session.request(this, request);
    }

    //----- Internal API

    StreamSenderOptions options() {
        return this.options;
    }

    @Override
    protected StreamSender self() {
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

    private StreamTracker sendMessage(AdvancedMessage<?> message, Map<String, Object> deliveryAnnotations, boolean waitForCredit) throws ClientException {
        final ClientFuture<StreamTracker> operation = session.getFutureFactory().createFuture();
        final ProtonBuffer buffer = message.encode(deliveryAnnotations);

        executor.execute(() -> {
            if (notClosedOrFailed(operation)) {
                try {
                    final ClientOutgoingEnvelope envelope = new ClientOutgoingEnvelope(this, null, message.messageFormat(), buffer, true, operation);

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

    StreamTracker sendMessage(ClientStreamSenderMessage context, ProtonBuffer payload, int messageFormat) throws ClientException {
        checkClosedOrFailed();

        final ClientFuture<StreamTracker> operation = session.getFutureFactory().createFuture();
        final ProtonBuffer buffer = payload;
        final ClientOutgoingEnvelope envelope = new ClientOutgoingEnvelope(
            this, context.getProtonDelivery(), messageFormat, buffer, context.completed(), operation);

        executor.execute(() -> {
            if (notClosedOrFailed(operation, context.getProtonDelivery().getLink())) {
                try {
                    if (protonSender.isSendable()) {
                        session.getTransactionContext().send(envelope, null, isSendingSettled());
                    } else {
                        addToHeadOfBlockedQueue(envelope);
                    }
                } catch (Exception error) {
                    operation.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
                }
            }
        });

        return session.request(this, operation);
    }

    private ClientStreamTracker createTracker(OutgoingDelivery delivery) {
        return new ClientStreamTracker(this, delivery);
    }

    private ClientNoOpStreamTracker createNoOpTracker() {
        return new ClientNoOpStreamTracker(this);
    }

    @Override
    void disposition(OutgoingDelivery delivery, DeliveryState state, boolean settled) throws ClientException {
        checkClosedOrFailed();
        executor.execute(() -> {
            delivery.disposition(state, settled);
        });
    }

    void abort(OutgoingDelivery delivery, ClientStreamTracker tracker) throws ClientException {
        checkClosedOrFailed();
        ClientFuture<StreamTracker> request = session().getFutureFactory().createFuture(new ClientSynchronization<StreamTracker>() {

            @Override
            public void onPendingSuccess(StreamTracker result) {
                handleCreditStateUpdated(protonLink());
            }

            @Override
            public void onPendingFailure(Throwable cause) {
                handleCreditStateUpdated(protonLink());
            }
        });

        executor.execute(() -> {
            if (delivery.getTransferCount() == 0) {
                delivery.abort();
                request.complete(tracker);
            } else {
                ClientOutgoingEnvelope envelope = new ClientOutgoingEnvelope(this, delivery, delivery.getMessageFormat(), null, false, request).abort();
                try {
                    if (protonSender.isSendable() && (protonSender.current() == null || protonSender.current() == delivery)) {
                        envelope.send(delivery.getState(), delivery.isSettled());
                    } else {
                        if (protonSender.current() == delivery) {
                            addToHeadOfBlockedQueue(envelope);
                        } else {
                            addToTailOfBlockedQueue(envelope);
                        }
                    }
                } catch (Exception error) {
                    request.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
                }
            }
        });

        session.request(this, request);
    }

    void complete(OutgoingDelivery delivery, ClientStreamTracker tracker) throws ClientException {
        checkClosedOrFailed();
        ClientFuture<StreamTracker> request = session().getFutureFactory().createFuture(new ClientSynchronization<StreamTracker>() {

            @Override
            public void onPendingSuccess(StreamTracker result) {
                handleCreditStateUpdated(protonLink());
            }

            @Override
            public void onPendingFailure(Throwable cause) {
                handleCreditStateUpdated(protonLink());
            }
        });

        executor.execute(() -> {
            ClientOutgoingEnvelope envelope = new ClientOutgoingEnvelope(this, delivery, delivery.getMessageFormat(), null, true, request);
            try {
                if (protonSender.isSendable() && (protonSender.current() == null || protonSender.current() == delivery)) {
                    envelope.send(delivery.getState(), delivery.isSettled());
                } else {
                    if (protonSender.current() == delivery) {
                        addToHeadOfBlockedQueue(envelope);
                    } else {
                        addToTailOfBlockedQueue(envelope);
                    }
                }
            } catch (Exception error) {
                request.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        session.request(this, request);
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
        // If the parent of this sender is a stream session than this sender owns it
        // and must close it when it closes itself to ensure that the resources are
        // cleaned up on the remote for the session.
        if (session instanceof ClientStreamSession) {
            session.closeAsync();
        }

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

    public static final class ClientOutgoingEnvelope implements ClientTransactionContext.Sendable {

        private final ProtonBuffer payload;
        private final ClientFuture<StreamTracker> request;
        private final ClientStreamSender sender;
        private final boolean complete;
        private final int messageFormat;

        private boolean aborted;
        private Future<?> sendTimeout;
        private OutgoingDelivery delivery;

        /**
         * Create a new In-flight Send instance that is a continuation on an existing delivery.
         *
         * @param sender
         *      The {@link ClientSender} instance that is attempting to send this encoded message.
         * @param messageFormat
         *      The message format code to assign the send if this is the first delivery.
         * @param delivery
         *      The {@link OutgoingDelivery} context this envelope will be added to.
         * @param payload
         *      The payload that comprises this portion of the send.
         * @param complete
         *      Indicates if the encoded payload represents the complete transfer or if more is coming.
         * @param request
         *      The requesting operation that initiated this send.
         */
        public ClientOutgoingEnvelope(ClientStreamSender sender, OutgoingDelivery delivery, int messageFormat, ProtonBuffer payload, boolean complete, ClientFuture<StreamTracker> request) {
            this.payload = payload;
            this.request = request;
            this.sender = sender;
            this.complete = complete;
            this.messageFormat = messageFormat;
            this.delivery = delivery;
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

        public ProtonBuffer payload() {
            return payload;
        }

        public OutgoingDelivery delivery() {
            return delivery;
        }

        public ClientOutgoingEnvelope abort() {
            this.aborted = true;
            return this;
        }

        public boolean aborted() {
            return aborted;
        }

        @Override
        public void discard() {
            if (sendTimeout != null) {
                sendTimeout.cancel(true);
                sendTimeout = null;
            }

            if (payload != null) {
                payload.close();
            }

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

        public ClientOutgoingEnvelope succeeded() {
            if (sendTimeout != null) {
                sendTimeout.cancel(true);
            }

            if (payload != null) {
                payload.close();
            }

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

            if (payload != null) {
                payload.close();
            }

            request.failed(exception);

            return this;
        }

        @Override
        public void send(DeliveryState state, boolean settled) {
            if (delivery == null) {
                delivery = sender.protonLink().next();
                delivery.setLinkedResource(sender.createTracker(delivery));
            }

            if (delivery.getTransferCount() == 0) {
                delivery.setMessageFormat(messageFormat);
                delivery.disposition(state, settled);
            }

            // We must check if the delivery was fully written and then complete the send operation otherwise
            // if the session capacity limited the amount of payload data we need to hold the completion until
            // the session capacity is refilled and we can fully write the remaining message payload.  This
            // area could use some enhancement to allow control of write and flush when dealing with delivery
            // modes that have low assurance versus those that are strict.
            if (aborted()) {
                delivery.abort();
                succeeded();
            } else {
                boolean wasAutoFlushOn = sender.connection().autoFlushOff();
                try {
                    delivery.streamBytes(payload, complete);
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
        }

        public ClientException createSendTimedOutException() {
            return new ClientSendTimedOutException("Timed out waiting for credit to send");
        }
    }
}

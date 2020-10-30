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

import java.util.Map;
import java.util.concurrent.ScheduledFuture;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.client.AdvancedMessage;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.StreamSender;
import org.apache.qpid.protonj2.client.StreamSenderOptions;
import org.apache.qpid.protonj2.client.StreamTracker;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.client.exceptions.ClientSendTimedOutException;
import org.apache.qpid.protonj2.client.futures.ClientFuture;
import org.apache.qpid.protonj2.engine.OutgoingDelivery;
import org.apache.qpid.protonj2.engine.util.StringUtils;
import org.apache.qpid.protonj2.types.messaging.DeliveryAnnotations;

public class ClientStreamSender extends ClientAbstractSender implements StreamSender {

    private final StreamSenderOptions options;

    public ClientStreamSender(ClientSession session, StreamSenderOptions options, String senderId, org.apache.qpid.protonj2.engine.Sender protonSender) {
        super(session, options, senderId, protonSender);

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
        final ClientFuture<ClientStreamSenderMessage> tracker = session.getFutureFactory().createFuture();
        final DeliveryAnnotations annotations;

        if (deliveryAnnotations != null) {
            annotations = new DeliveryAnnotations(StringUtils.toSymbolKeyedMap(deliveryAnnotations));
        } else {
            annotations = null;
        }

        executor.execute(() -> {
            if (protonSender.current() != null) {
                tracker.failed(new ClientIllegalStateException(
                    "Cannot initiate a new streaming send until the previous one is complete"));
            } else {
                tracker.complete(new ClientStreamSenderMessage(this, protonSender.next(), annotations));
            }
        });

        return session.request(this, tracker);
    }

    //----- Internal API

    StreamSenderOptions options() {
        return this.options;
    }

    @Override
    ClientStreamSender open() {
        return (ClientStreamSender) super.open();
    }

    void sendMessage(ClientStreamSenderMessage context, AdvancedMessage<?> message) throws ClientException {
        final ClientFuture<ClientStreamTracker> operation = session.getFutureFactory().createFuture();
        final ProtonBuffer buffer = message.encode(null);

        executor.execute(() -> {
            checkClosedOrFailed(operation);

            try {
                final InFlightSend send = new SenderInFlightSend(message.messageFormat(), context, buffer, operation);

                if (protonSender.isSendable()) {
                    try {
                        assumeSendableAndSend(send);
                    } catch (Exception ex) {
                        operation.failed(ClientExceptionSupport.createNonFatalOrPassthrough(ex));
                    }
                } else {
                    addToTailOfBlockedQueue(send);
                }
            } catch (Exception error) {
                operation.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        session.request(this, operation);
    }

    @Override
    protected StreamTracker sendMessage(AdvancedMessage<?> message, Map<String, Object> deliveryAnnotations, boolean waitForCredit) throws ClientException {
        final ClientFuture<ClientStreamTracker> operation = session.getFutureFactory().createFuture();
        final ProtonBuffer buffer = message.encode(deliveryAnnotations);

        executor.execute(() -> {
            checkClosedOrFailed(operation);

            try {
                final InFlightSend send = new SenderInFlightSend(message.messageFormat(), null, buffer, operation);

                if (protonSender.isSendable() && protonSender.current() == null) {
                    try {
                        assumeSendableAndSend(send);
                    } catch (Exception ex) {
                        operation.failed(ClientExceptionSupport.createNonFatalOrPassthrough(ex));
                    }
                } else if (waitForCredit) {
                    addToTailOfBlockedQueue(send);
                } else {
                    operation.complete(null);
                }
            } catch (Exception error) {
                operation.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return session.request(this, operation);
    }

    private class SenderInFlightSend implements ClientAbstractSender.InFlightSend {

        private final int messageFormat;
        private final ProtonBuffer encodedMessage;
        private final ClientFuture<ClientStreamTracker> operation;
        private final ClientStreamSenderMessage context;

        private ScheduledFuture<?> timeout;
        private ClientStreamTracker tracker;
        private OutgoingDelivery delivery;

        public SenderInFlightSend(int messageFormat, ClientStreamSenderMessage context, ProtonBuffer encodedMessage, ClientFuture<ClientStreamTracker> operation) {
            this.context = context;
            this.delivery = context != null ? context.protonDelivery() : null;
            this.messageFormat = messageFormat;
            this.encodedMessage = encodedMessage;
            this.operation = operation;
        }

        @Override
        public OutgoingDelivery delivery() {
            if (delivery == null) {
                delivery = protonSender.next();
                delivery.setMessageFormat(messageFormat);
                delivery.setLinkedResource(tracker = new ClientStreamTracker(ClientStreamSender.this, delivery));
            } else {
                delivery.setMessageFormat(messageFormat);
            }

            return delivery;
        }

        @Override
        public ClientStreamTracker tracker() {
            return tracker;
        }

        @Override
        public boolean aborted() {
            return context != null ? context.aborted() : false;
        }

        @Override
        public boolean complete() {
            return context != null ? context.completed() : true;
        }

        @Override
        public InFlightSend failed(ClientException result) {
            if (timeout != null) {
                timeout.cancel(true);
            }
            operation.failed(result);
            return this;
        }

        @Override
        public void ignored() {
            tracker.settlementFuture().complete(tracker);
            operation.complete(tracker);
        }

        @Override
        public void succeeded() {
            operation.complete(tracker);
        }

        @Override
        public ClientException createSendTimedOutException() {
            return new ClientSendTimedOutException("Timed out waiting for credit to send");
        }

        @Override
        public ScheduledFuture<?> timeout() {
            return timeout;
        }

        @Override
        public void timeout(ScheduledFuture<?> sendTimeout) {
            timeout = sendTimeout;
        }

        @Override
        public ProtonBuffer encodedMessage() {
            return encodedMessage;
        }
    }
}

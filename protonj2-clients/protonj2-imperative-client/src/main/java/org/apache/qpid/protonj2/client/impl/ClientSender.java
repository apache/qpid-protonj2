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
import org.apache.qpid.protonj2.client.Sender;
import org.apache.qpid.protonj2.client.SenderOptions;
import org.apache.qpid.protonj2.client.Tracker;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientSendTimedOutException;
import org.apache.qpid.protonj2.client.futures.ClientFuture;
import org.apache.qpid.protonj2.engine.OutgoingDelivery;

/**
 * Proton based AMQP Sender
 */
public class ClientSender extends ClientAbstractSender implements Sender {

    public ClientSender(ClientSession session, SenderOptions options, String senderId, org.apache.qpid.protonj2.engine.Sender protonSender) {
        super(session, options, senderId, protonSender);
    }

    @Override
    ClientSender open() {
        return (ClientSender) super.open();
    }

    @Override
    protected Tracker sendMessage(AdvancedMessage<?> message, Map<String, Object> deliveryAnnotations, boolean waitForCredit) throws ClientException {
        final ClientFuture<ClientTracker> operation = session.getFutureFactory().createFuture();
        final ProtonBuffer buffer = message.encode(deliveryAnnotations);

        executor.execute(() -> {
            checkClosedOrFailed(operation);

            try {
                final InFlightSend send = new SenderInFlightSend(message.messageFormat(), buffer, operation);

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
        private final ClientFuture<ClientTracker> operation;

        private ScheduledFuture<?> timeout;
        private ClientTracker tracker;
        private OutgoingDelivery delivery;

        public SenderInFlightSend(int messageFormat, ProtonBuffer encodedMessage, ClientFuture<ClientTracker> operation) {
            this.messageFormat = messageFormat;
            this.encodedMessage = encodedMessage;
            this.operation = operation;
        }

        @Override
        public OutgoingDelivery delivery() {
            if (delivery == null) {
                delivery = protonSender.next();
                delivery.setMessageFormat(messageFormat);
                delivery.setLinkedResource(tracker = new ClientTracker(ClientSender.this, delivery));
            }

            return delivery;
        }

        @Override
        public ClientTracker tracker() {
            return tracker;
        }

        @Override
        public boolean aborted() {
            return false;
        }

        @Override
        public boolean complete() {
            return true;
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

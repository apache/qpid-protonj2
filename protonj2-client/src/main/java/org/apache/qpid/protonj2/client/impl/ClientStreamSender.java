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

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.client.AdvancedMessage;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.StreamSender;
import org.apache.qpid.protonj2.client.StreamSenderOptions;
import org.apache.qpid.protonj2.client.StreamTracker;
import org.apache.qpid.protonj2.client.Tracker;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.client.futures.ClientFuture;
import org.apache.qpid.protonj2.engine.OutgoingDelivery;
import org.apache.qpid.protonj2.engine.util.StringUtils;
import org.apache.qpid.protonj2.types.messaging.DeliveryAnnotations;

public final class ClientStreamSender extends ClientSender implements StreamSender {

    private final StreamSenderOptions options;

    public ClientStreamSender(ClientSession session, StreamSenderOptions options, String senderId, org.apache.qpid.protonj2.engine.Sender protonSender) {
        super(session, options, senderId, protonSender);

        this.options = new StreamSenderOptions(options);
    }

    @Override
    public StreamTracker send(Message<?> message) throws ClientException {
        checkClosedOrFailed();
        return (StreamTracker) sendMessage(ClientMessageSupport.convertMessage(message), null, true);
    }

    @Override
    public StreamTracker send(Message<?> message, Map<String, Object> deliveryAnnotations) throws ClientException {
        checkClosedOrFailed();
        return (StreamTracker) sendMessage(ClientMessageSupport.convertMessage(message), null, true);
    }

    @Override
    public StreamTracker trySend(Message<?> message) throws ClientException {
        checkClosedOrFailed();
        return (StreamTracker) sendMessage(ClientMessageSupport.convertMessage(message), null, false);
    }

    @Override
    public StreamTracker trySend(Message<?> message, Map<String, Object> deliveryAnnotations) throws ClientException {
        checkClosedOrFailed();
        return (StreamTracker) sendMessage(ClientMessageSupport.convertMessage(message), null, false);
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

    @Override
    StreamSenderOptions options() {
        return this.options;
    }

    @Override
    ClientStreamSender open() {
        return (ClientStreamSender) super.open();
    }

    StreamTracker sendMessage(ClientStreamSenderMessage context, AdvancedMessage<?> message) throws ClientException {
        final ClientFuture<Tracker> operation = session.getFutureFactory().createFuture();
        final ProtonBuffer buffer = message.encode(null);
        final ClientOutgoingEnvelope envelope = new ClientOutgoingEnvelope(
            this, context.getProtonDelivery(), message.messageFormat(), buffer, context.completed(), operation);

        executor.execute(() -> {
            checkClosedOrFailed(operation);

            try {
                if (protonSender.isSendable()) {
                    try {
                        envelope.sendPayload();
                    } catch (Exception ex) {
                        operation.failed(ClientExceptionSupport.createNonFatalOrPassthrough(ex));
                    }
                } else {
                    addToTailOfBlockedQueue(envelope);
                }
            } catch (Exception error) {
                operation.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return (StreamTracker) session.request(this, operation);
    }

    @Override
    protected ClientStreamTracker createTracker(OutgoingDelivery delivery) {
        return new ClientStreamTracker(this, delivery);
    }
}

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

import java.util.concurrent.ScheduledFuture;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.client.Tracker;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientSendTimedOutException;
import org.apache.qpid.protonj2.client.futures.ClientFuture;
import org.apache.qpid.protonj2.engine.OutgoingDelivery;
import org.apache.qpid.protonj2.engine.Sender;
import org.apache.qpid.protonj2.types.transport.DeliveryState;

/**
 * Tracking object used to manage the life-cycle of a send of message payload
 * to the remote which can be stalled either for link or session credit limits.
 * The envelope carries sufficient information to write payload bytes as credit
 * is available.
 */
public class ClientOutgoingEnvelope {

    private final ProtonBuffer payload;
    private final ClientFuture<Tracker> request;
    private final ClientSender sender;
    private final Sender protonSender;
    private final boolean complete;
    private final int messageFormat;

    private ScheduledFuture<?> sendTimeout;
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
    public ClientOutgoingEnvelope(ClientSender sender, int messageFormat, ProtonBuffer payload, ClientFuture<Tracker> request) {
        this.messageFormat = messageFormat;
        this.payload = payload;
        this.request = request;
        this.sender = sender;
        this.protonSender = sender.getProtonSender();
        this.complete = true;
    }

    /**
     * Create a new In-flight Send instance.
     *
     * @param sender
     *      The {@link ClientSender} instance that is attempting to send this encoded message.
     * @param messageFormat
     *      The message format code to assign the send if this is the first delivery.
     * @param payload
     *      The payload that comprises this portion of the send.
     * @param complete
     *      Indicates if the encoded payload represents the complete transfer or if more is coming.
     * @param request
     *      The requesting operation that initiated this send.
     */
    public ClientOutgoingEnvelope(ClientSender sender, int messageFormat, ProtonBuffer payload, boolean complete, ClientFuture<Tracker> request) {
        this.payload = payload;
        this.request = request;
        this.sender = sender;
        this.protonSender = sender.getProtonSender();
        this.complete = complete;
        this.messageFormat = messageFormat;
    }

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
    public ClientOutgoingEnvelope(ClientSender sender, OutgoingDelivery delivery, int messageFormat, ProtonBuffer payload, boolean complete, ClientFuture<Tracker> request) {
        this.payload = payload;
        this.request = request;
        this.sender = sender;
        this.protonSender = sender.getProtonSender();
        this.complete = complete;
        this.messageFormat = messageFormat;
        this.delivery = delivery;
    }

    public ScheduledFuture<?> sendTimeout() {
        return sendTimeout;
    }

    public void sendTimeout(ScheduledFuture<?> sendTimeout) {
        this.sendTimeout = sendTimeout;
    }

    public ProtonBuffer payload() {
        return payload;
    }

    public OutgoingDelivery delivery() {
        return delivery;
    }

    public boolean isComplete() {
        return complete;
    }

    public ClientOutgoingEnvelope discard() {
        if (sendTimeout != null) {
            sendTimeout.cancel(true);
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

        return this;
    }

    public ClientOutgoingEnvelope succeeded() {
        if (sendTimeout != null) {
            sendTimeout.cancel(true);
        }

        request.complete(delivery.getLinkedResource());

        return this;
    }

    public ClientOutgoingEnvelope failed(ClientException exception) {
        if (sendTimeout != null) {
            sendTimeout.cancel(true);
        }

        request.failed(exception);

        return this;
    }

    public void sendPayload(DeliveryState state, boolean settled) {
        if (delivery == null) {
            delivery = protonSender.next();
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
        delivery.streamBytes(payload, complete);
        if (payload.isReadable()) {
            sender.addToHeadOfBlockedQueue(this);
        } else {
            succeeded();
        }
    }

    public ClientException createSendTimedOutException() {
        return new ClientSendTimedOutException("Timed out waiting for credit to send");
    }
}

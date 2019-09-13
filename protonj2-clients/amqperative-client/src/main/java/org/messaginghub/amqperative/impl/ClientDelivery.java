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

import org.apache.qpid.proton4j.amqp.messaging.Accepted;
import org.apache.qpid.proton4j.engine.IncomingDelivery;
import org.messaginghub.amqperative.Delivery;
import org.messaginghub.amqperative.DeliveryState;
import org.messaginghub.amqperative.Message;
import org.messaginghub.amqperative.Receiver;

/**
 * Client inbound delivery object.
 */
public class ClientDelivery implements Delivery {

    private final ClientReceiver receiver;
    private final IncomingDelivery delivery;

    private Message<?> cachedMessage;

    /**
     * Creates a new client delivery object linked to the given {@link IncomingDelivery}
     * instance.
     *
     * @param receiver
     *      The {@link Receiver} that processed this delivery.
     * @param delivery
     *      The proton incoming delivery that backs this client delivery facade.
     */
    ClientDelivery(ClientReceiver receiver, IncomingDelivery delivery) {
        this.receiver = receiver;
        this.delivery = delivery;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <E> Message<E> getMessage() throws ClientException {
        if (delivery.isPartial()) {
            // TODO - Client exception of some sort, ClientPartialMessageException etc
            throw new IllegalStateException("Message is still only partially delivered.");
        }

        Message<E> message = (Message<E>) cachedMessage;
        if (message == null && delivery.available() > 0) {
            message = (Message<E>) (cachedMessage = ClientMessageSupport.decodeMessage(delivery.readAll()));
        }

        return message;
    }

    @Override
    public Delivery accept() {
        receiver.disposition(delivery, Accepted.getInstance(), true);  // TODO - Are we Auto settling ?
        return this;
    }

    @Override
    public Delivery disposition(DeliveryState state, boolean settle) {
        org.apache.qpid.proton4j.amqp.transport.DeliveryState protonState = null;
        if (state != null) {
            try {
                protonState = ((ClientDeliveryState) state).getProtonDeliveryState();
            } catch (ClassCastException ccex) {
                throw new IllegalArgumentException("Unknown DeliveryState type given, no disposition applied to Delivery.");
            }
        }

        receiver.disposition(delivery, protonState, settle);
        return this;
    }

    @Override
    public Delivery settle() {
        receiver.disposition(delivery, null, true);
        return this;
    }

    @Override
    public DeliveryState getState() {
        return ClientDeliveryState.fromProtonType(delivery.getLocalState());
    }

    @Override
    public DeliveryState getRemoteState() {
        return ClientDeliveryState.fromProtonType(delivery.getRemoteState());
    }

    // TODO: Additional note about hiding this away someplace where it is less likely to
    //       confuse / harm the caller since it isn't really telling of actual remote settled.

    @Override
    public boolean isRemotelySettled() {
        return delivery.isRemotelySettled();
    }

    @Override
    public byte[] getTag() {
        return delivery.getTag();
    }

    @Override
    public int getMessageFormat() {
        return delivery.getMessageFormat();
    }

    @Override
    public Receiver getReceiver() {
        return receiver;
    }

    @Override
    public boolean isSettled() {
        return delivery.isSettled();
    }
}

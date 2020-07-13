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

import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.DeliveryState;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientPartialMessageException;
import org.apache.qpid.protonj2.engine.IncomingDelivery;
import org.apache.qpid.protonj2.types.messaging.Accepted;
import org.apache.qpid.protonj2.types.messaging.Modified;
import org.apache.qpid.protonj2.types.messaging.Rejected;
import org.apache.qpid.protonj2.types.messaging.Released;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;

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
    public <E> Message<E> message() throws ClientException {
        if (delivery.isPartial()) {
            throw new ClientPartialMessageException("Delivery contains only a partial amount of the message payload.");
        }

        Message<E> message = (Message<E>) cachedMessage;
        if (message == null && delivery.available() > 0) {
            message = (Message<E>) (cachedMessage = ClientMessageSupport.decodeMessage(delivery.readAll()));
        }

        return message;
    }

    @Override
    public Delivery accept() throws ClientException {
        receiver.disposition(delivery, Accepted.getInstance(), true);
        return this;
    }

    @Override
    public Delivery release() throws ClientException {
        receiver.disposition(delivery, Released.getInstance(), true);
        return this;
    }

    @Override
    public Delivery reject(String condition, String description) throws ClientException {
        receiver.disposition(delivery, new Rejected().setError(new ErrorCondition(condition, description)), true);
        return this;
    }

    @Override
    public Delivery modified(boolean deliveryFailed, boolean undeliverableHere) throws ClientException {
        receiver.disposition(delivery, new Modified().setDeliveryFailed(deliveryFailed).setUndeliverableHere(undeliverableHere), true);
        return this;
    }

    @Override
    public Delivery disposition(DeliveryState state, boolean settle) throws ClientException {
        org.apache.qpid.protonj2.types.transport.DeliveryState protonState = null;
        if (state != null) {
            protonState = ClientDeliveryState.asProtonType(state);
        }

        receiver.disposition(delivery, protonState, settle);
        return this;
    }

    @Override
    public Delivery settle() throws ClientException {
        receiver.disposition(delivery, null, true);
        return this;
    }

    @Override
    public DeliveryState state() {
        return ClientDeliveryState.fromProtonType(delivery.getState());
    }

    @Override
    public DeliveryState remoteState() {
        return ClientDeliveryState.fromProtonType(delivery.getRemoteState());
    }

    @Override
    public boolean remoteSettled() {
        return delivery.isRemotelySettled();
    }

    @Override
    public int messageFormat() {
        return delivery.getMessageFormat();
    }

    @Override
    public Receiver receiver() {
        return receiver;
    }

    @Override
    public boolean settled() {
        return delivery.isSettled();
    }
}

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

import java.io.InputStream;
import java.util.Map;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferInputStream;
import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.DeliveryState;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.engine.IncomingDelivery;
import org.apache.qpid.protonj2.engine.util.StringUtils;
import org.apache.qpid.protonj2.types.messaging.Accepted;
import org.apache.qpid.protonj2.types.messaging.DeliveryAnnotations;
import org.apache.qpid.protonj2.types.messaging.Modified;
import org.apache.qpid.protonj2.types.messaging.Rejected;
import org.apache.qpid.protonj2.types.messaging.Released;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;

/**
 * Client inbound delivery object.
 */
public final class ClientDelivery implements Delivery {

    private final ClientReceiver receiver;
    private final IncomingDelivery delivery;
    private final ProtonBuffer payload;

    private DeliveryAnnotations deliveryAnnotations;
    private Message<?> cachedMessage;
    private InputStream rawInputStream;

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
        this.delivery.setLinkedResource(this);
        this.payload = delivery.readAll();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <E> Message<E> message() throws ClientException {
        if (rawInputStream != null) {
            throw new ClientIllegalStateException("Cannot access Delivery Annotations API after requesting an InputStream");
        }

        Message<E> message = (Message<E>) cachedMessage;
        if (message == null && payload.isReadable()) {
            message = (Message<E>)(cachedMessage = ClientMessageSupport.decodeMessage(payload, this::deliveryAnnotations));
        }

        return message;
    }

    @Override
    public InputStream rawInputStream() throws ClientException {
        if (cachedMessage != null) {
            throw new ClientIllegalStateException("Cannot access Delivery InputStream API after requesting an Message");
        }

        if (rawInputStream == null) {
            rawInputStream = new ProtonBufferInputStream(payload);
        }

        return rawInputStream;
    }

    @Override
    public Map<String, Object> annotations() throws ClientException {
        message();

        if (deliveryAnnotations != null && deliveryAnnotations.getValue() != null) {
            return StringUtils.toStringKeyedMap(deliveryAnnotations.getValue());
        } else {
            return null;
        }
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
        receiver.disposition(delivery, ClientDeliveryState.asProtonType(state), settle);
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

    //----- Internal API not meant to be used from outside the client package.

    IncomingDelivery protonDelivery() {
        return delivery;
    }

    void deliveryAnnotations(DeliveryAnnotations deliveryAnnotations) {
        this.deliveryAnnotations = deliveryAnnotations;
    }
}

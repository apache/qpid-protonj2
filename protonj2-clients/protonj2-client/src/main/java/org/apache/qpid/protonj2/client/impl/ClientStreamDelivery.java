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

import org.apache.qpid.protonj2.client.DeliveryState;
import org.apache.qpid.protonj2.client.StreamDelivery;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.engine.IncomingDelivery;
import org.apache.qpid.protonj2.engine.util.StringUtils;
import org.apache.qpid.protonj2.types.messaging.Accepted;
import org.apache.qpid.protonj2.types.messaging.Modified;
import org.apache.qpid.protonj2.types.messaging.Rejected;
import org.apache.qpid.protonj2.types.messaging.Released;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;

public class ClientStreamDelivery implements StreamDelivery {

    private final ClientStreamReceiver receiver;
    private final IncomingDelivery protonDelivery;

    private ClientStreamReceiverMessage message;
    private InputStream rawInputStream;

    public ClientStreamDelivery(ClientStreamReceiver receiver, IncomingDelivery protonDelivery) {
        this.receiver = receiver;
        this.protonDelivery = protonDelivery.setLinkedResource(this);
    }

    @Override
    public ClientStreamReceiver receiver() {
        return receiver;
    }

    @Override
    public boolean aborted() {
        return protonDelivery.isAborted();
    }

    @Override
    public boolean completed() {
        return !protonDelivery.isPartial();
    }

    @Override
    public int messageFormat() {
        return protonDelivery.getMessageFormat();
    }

    @Override
    public ClientStreamReceiverMessage message() throws ClientException {
        if (rawInputStream != null) {
            throw new ClientIllegalStateException("Cannot access Delivery Message API after requesting an InputStream");
        }

        if (message == null) {
            message = new ClientStreamReceiverMessage(receiver, protonDelivery);
        }

        return message;
    }

    @Override
    public Map<String, Object> annotations() throws ClientException {
        if (rawInputStream != null) {
            throw new ClientIllegalStateException("Cannot access Delivery Annotations API after requesting an InputStream");
        }

        if (message == null) {
            message = new ClientStreamReceiverMessage(receiver, protonDelivery);
        }

        return StringUtils.toStringKeyedMap(message.deliveryAnnotations() != null ? message.deliveryAnnotations().getValue() : null);
    }

    @Override
    public InputStream rawInputStream() throws ClientException {
        if (message != null) {
            throw new ClientIllegalStateException("Cannot access Delivery InputStream API after requesting an Message");
        }

        if (rawInputStream == null) {
            // TODO: Create stream for large message incoming bytes.
        }

        return rawInputStream;
    }

    @Override
    public StreamDelivery accept() throws ClientException {
        receiver.disposition(protonDelivery, Accepted.getInstance(), true);
        return this;
    }

    @Override
    public StreamDelivery release() throws ClientException {
        receiver.disposition(protonDelivery, Released.getInstance(), true);
        return this;
    }

    @Override
    public StreamDelivery reject(String condition, String description) throws ClientException {
        receiver.disposition(protonDelivery, new Rejected().setError(new ErrorCondition(condition, description)), true);
        return this;
    }

    @Override
    public StreamDelivery modified(boolean deliveryFailed, boolean undeliverableHere) throws ClientException {
        receiver.disposition(protonDelivery, new Modified().setDeliveryFailed(deliveryFailed).setUndeliverableHere(undeliverableHere), true);
        return this;
    }

    @Override
    public StreamDelivery disposition(DeliveryState state, boolean settle) throws ClientException {
        receiver.disposition(protonDelivery, ClientDeliveryState.asProtonType(state), settle);
        return this;
    }

    @Override
    public StreamDelivery settle() throws ClientException {
        receiver.disposition(protonDelivery, null, true);
        return this;
    }

    @Override
    public DeliveryState state() {
        return ClientDeliveryState.fromProtonType(protonDelivery.getState());
    }

    @Override
    public boolean settled() {
        return protonDelivery.isSettled();
    }

    @Override
    public DeliveryState remoteState() {
        return ClientDeliveryState.fromProtonType(protonDelivery.getRemoteState());
    }

    @Override
    public boolean remoteSettled() {
        return protonDelivery.isRemotelySettled();
    }
}

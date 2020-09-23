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

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.qpid.protonj2.client.DeliveryState;
import org.apache.qpid.protonj2.client.StreamReceiver;
import org.apache.qpid.protonj2.client.StreamReceiverMessage;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.engine.IncomingDelivery;
import org.apache.qpid.protonj2.types.messaging.Accepted;
import org.apache.qpid.protonj2.types.messaging.DeliveryAnnotations;
import org.apache.qpid.protonj2.types.messaging.Modified;
import org.apache.qpid.protonj2.types.messaging.Rejected;
import org.apache.qpid.protonj2.types.messaging.Released;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;
import org.apache.qpid.protonj2.types.transport.Transfer;

/**
 * Streamed message delivery context used to request reads of possible split framed
 * {@link Transfer} payload's that comprise a single large overall message.
 */
public class ClientStreamReceiverMessage implements StreamReceiverMessage {

    private final ClientStreamReceiver receiver;
    private final IncomingDelivery protonDelivery;

    private DeliveryAnnotations deliveryAnnotations;

    ClientStreamReceiverMessage(ClientStreamReceiver receiver, IncomingDelivery delivery) {
        this.receiver = receiver;
        this.protonDelivery = delivery.setLinkedResource(this);
    }

    @Override
    public StreamReceiver receiver() {
        return receiver;
    }

    @Override
    public Map<String, Object> annotations() throws ClientException {
        if (protonDelivery != null) {
            return null;
        } else {
            throw new ClientIllegalStateException("Cannot read message data until the remote begins a transfer");
        }
    }

    @Override
    public boolean aborted() {
        if (protonDelivery != null) {
            return protonDelivery.isAborted();
        } else {
            return false;
        }
    }

    @Override
    public boolean completed() {
        if (protonDelivery != null) {
            return !protonDelivery.isPartial() && !protonDelivery.isAborted();
        } else {
            return false;
        }
    }

    @Override
    public InputStream rawInputStream() throws ClientException {
        checkClosed();
        checkAborted();

        return new RawInputStream();
    }

    @Override
    public ClientStreamReceiverMessage accept() throws ClientException {
        disposition(Accepted.getInstance(), true);
        return this;
    }

    @Override
    public ClientStreamReceiverMessage release() throws ClientException {
        disposition(Released.getInstance(), true);
        return this;
    }

    @Override
    public ClientStreamReceiverMessage reject(String condition, String description) throws ClientException {
        disposition(new Rejected().setError(new ErrorCondition(condition, description)), true);
        return this;
    }

    @Override
    public ClientStreamReceiverMessage modified(boolean deliveryFailed, boolean undeliverableHere) throws ClientException {
        disposition(new Modified().setDeliveryFailed(deliveryFailed).setUndeliverableHere(undeliverableHere), true);
        return this;
    }

    @Override
    public ClientStreamReceiverMessage disposition(DeliveryState state, boolean settle) throws ClientException {
        disposition(ClientDeliveryState.asProtonType(state), settle);
        return this;
    }

    @Override
    public ClientStreamReceiverMessage settle() throws ClientException {
        disposition(protonDelivery.getState(), true);
        return this;
    }

    @Override
    public boolean settled() {
        return protonDelivery != null ? protonDelivery.isSettled() : false;
    }

    @Override
    public DeliveryState state() {
        return protonDelivery != null ? ClientDeliveryState.fromProtonType(protonDelivery.getState()) : null;
    }

    @Override
    public int messageFormat() {
        return protonDelivery != null ? protonDelivery.getMessageFormat() : 0;
    }

    @Override
    public DeliveryState remoteState() {
        return protonDelivery != null ? ClientDeliveryState.fromProtonType(protonDelivery.getRemoteState()) : null;
    }

    @Override
    public boolean remoteSettled() {
        return protonDelivery != null ? protonDelivery.isRemotelySettled() : false;
    }

    //----- Internal Streamed Delivery API and support methods

    IncomingDelivery protonDelivery() {
        return protonDelivery;
    }

    void deliveryAnnotations(DeliveryAnnotations deliveryAnnotations) {
        this.deliveryAnnotations = deliveryAnnotations;
    }

    private void checkClosed() throws ClientIllegalStateException {
        if (receiver.isClosed()) {
            throw new ClientIllegalStateException("The parent Receiver instance has already been closed.");
        }
    }

    private void checkAborted() throws ClientIllegalStateException {
        if (aborted()) {
            throw new ClientIllegalStateException("The incoming delivery was aborted.");
        }
    }

    private void disposition(org.apache.qpid.protonj2.types.transport.DeliveryState state, boolean settle) throws ClientException {
        checkAborted();
        if (protonDelivery != null) {
            receiver.disposition(protonDelivery, state, settle);
        }
    }

    //----- Event Handlers for Delivery updates

    void handleDeliveryRead(IncomingDelivery delivery) {
        // TODO: break any waiting for read cases
    }

    void handleDeliveryAborted(IncomingDelivery delivery) {
        // TODO: break any waiting for read cases
    }

    void handleReceiverClosed(ClientStreamReceiver receiver) {
        // TODO: break any waiting for read cases
    }

    //----- Internal InputStream implementations

    @SuppressWarnings("unused")
    private class RawInputStream extends InputStream {

        protected final AtomicBoolean closed = new AtomicBoolean();

        @Override
        public int read() throws IOException {
            throw new IOException();
        }

        private void checkClosed() throws IOException {
            if (closed.get()) {
                throw new IOException("The InputStream has already been closed.");
            }

            if (receiver.isClosed()) {
                throw new IOException("The parent Receiver instance has already been closed.");
            }
        }
    }
}

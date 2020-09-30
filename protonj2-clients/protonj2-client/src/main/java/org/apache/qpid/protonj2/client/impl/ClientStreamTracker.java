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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.qpid.protonj2.client.DeliveryState;
import org.apache.qpid.protonj2.client.StreamTracker;
import org.apache.qpid.protonj2.client.Tracker;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientOperationTimedOutException;
import org.apache.qpid.protonj2.client.futures.ClientFuture;
import org.apache.qpid.protonj2.engine.OutgoingDelivery;

public class ClientStreamTracker implements StreamTracker {

    private final ClientStreamSender sender;
    private final ClientStreamSenderMessage message;
    private final OutgoingDelivery delivery;

    private final ClientFuture<Tracker> acknowledged;

    private volatile boolean remotelySetted;
    private volatile DeliveryState remoteDeliveryState;

    public ClientStreamTracker(ClientStreamSenderMessage message) {
        this.message = message;
        this.sender = message.sender();
        this.delivery = message.protonDelivery();
        this.acknowledged = sender.session().getFutureFactory().createFuture();
    }

    OutgoingDelivery delivery() {
        return delivery;
    }

    @Override
    public ClientStreamSenderMessage message() {
        return message;
    }

    @Override
    public ClientStreamSender sender() {
        return sender;
    }

    @Override
    public DeliveryState state() {
        return ClientDeliveryState.fromProtonType(delivery.getState());
    }

    @Override
    public DeliveryState remoteState() {
        return remoteDeliveryState;
    }

    @Override
    public boolean remoteSettled() {
        return remotelySetted;
    }

    @Override
    public ClientStreamTracker disposition(DeliveryState state, boolean settle) throws ClientException {
        org.apache.qpid.protonj2.types.transport.DeliveryState protonState = null;
        if (state != null) {
            protonState = ClientDeliveryState.asProtonType(state);
        }

        sender.disposition(delivery, protonState, settle);
        return this;
    }

    @Override
    public ClientStreamTracker settle() throws ClientException {
        sender.disposition(delivery, null, true);
        return this;
    }

    @Override
    public boolean settled() {
        return delivery.isSettled();
    }

    @Override
    public ClientFuture<Tracker> settlementFuture() {
        return acknowledged;
    }

    @Override
    public StreamTracker awaitSettlement() throws ClientException {
        try {
            settlementFuture().get();
            return this;
        } catch (ExecutionException exe) {
            throw ClientExceptionSupport.createNonFatalOrPassthrough(exe.getCause());
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw new ClientException("Wait for settlement was interrupted", e);
        }
    }

    @Override
    public StreamTracker awaitSettlement(long timeout, TimeUnit unit) throws ClientException {
        try {
            settlementFuture().get(timeout, unit);
            return this;
        } catch (InterruptedException ie) {
            Thread.interrupted();
            throw new ClientException("Wait for settlement was interrupted", ie);
        } catch (ExecutionException exe) {
            throw ClientExceptionSupport.createNonFatalOrPassthrough(exe.getCause());
        } catch (TimeoutException te) {
            throw new ClientOperationTimedOutException("Timed out waiting for remote settlement", te);
        }
    }

    //----- Internal Event hooks for delivery updates

    void processDeliveryUpdated(OutgoingDelivery delivery) {
        remotelySetted = delivery.isRemotelySettled();
        remoteDeliveryState = ClientDeliveryState.fromProtonType(delivery.getRemoteState());

        if (delivery.isRemotelySettled()) {
            acknowledged.complete(this);
        }
    }
}

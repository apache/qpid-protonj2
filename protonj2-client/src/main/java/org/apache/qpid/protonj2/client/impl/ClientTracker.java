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
import org.apache.qpid.protonj2.client.Sender;
import org.apache.qpid.protonj2.client.Tracker;
import org.apache.qpid.protonj2.client.exceptions.ClientDeliveryStateException;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientOperationTimedOutException;
import org.apache.qpid.protonj2.client.futures.ClientFuture;
import org.apache.qpid.protonj2.engine.OutgoingDelivery;

/**
 * Client outgoing delivery tracker object.
 */
class ClientTracker implements Tracker {

    private final ClientSender sender;
    private final OutgoingDelivery delivery;

    private final ClientFuture<Tracker> remoteSettlementFuture;

    private volatile boolean remotelySettled;
    private volatile DeliveryState remoteDeliveryState;

    /**
     * Create an instance of a client outgoing delivery tracker.
     *
     * @param sender
     *      The sender that was used to send the delivery
     * @param delivery
     *      The proton outgoing delivery object that backs this tracker.
     */
    ClientTracker(ClientSender sender, OutgoingDelivery delivery) {
        this.sender = sender;
        this.delivery = delivery;
        this.delivery.deliveryStateUpdatedHandler(this::processDeliveryUpdated);
        this.remoteSettlementFuture = sender.session().getFutureFactory().createFuture();
    }

    OutgoingDelivery delivery() {
        return delivery;
    }

    @Override
    public Sender sender() {
        return sender;
    }

    @Override
    public synchronized DeliveryState state() {
        return ClientDeliveryState.fromProtonType(delivery.getState());
    }

    @Override
    public DeliveryState remoteState() {
        return remoteDeliveryState;
    }

    @Override
    public boolean remoteSettled() {
        return remotelySettled;
    }

    @Override
    public Tracker disposition(DeliveryState state, boolean settle) throws ClientException {
        try {
            sender.disposition(delivery, ClientDeliveryState.asProtonType(state), settle);
        } finally {
            if (settle) {
                remoteSettlementFuture.complete(this);
            }
        }

        return this;
    }

    @Override
    public Tracker settle() throws ClientException {
        try {
            sender.disposition(delivery, null, true);
        } finally {
            remoteSettlementFuture.complete(this);
        }

        return this;
    }

    @Override
    public synchronized boolean settled() {
        return delivery.isSettled();
    }

    @Override
    public ClientFuture<Tracker> settlementFuture() {
        if (delivery.isSettled()) {
            remoteSettlementFuture.complete(this);
        }

        return remoteSettlementFuture;
    }

    @Override
    public Tracker awaitSettlement() throws ClientException {
        try {
            if (settled()) {
                return this;
            } else {
                return settlementFuture().get();
            }
        } catch (ExecutionException exe) {
            throw ClientExceptionSupport.createNonFatalOrPassthrough(exe.getCause());
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw new ClientException("Wait for settlement was interrupted", e);
        }
    }

    @Override
    public Tracker awaitSettlement(long timeout, TimeUnit unit) throws ClientException {
        try {
            if (settled()) {
                return this;
            } else {
                return settlementFuture().get(timeout, unit);
            }
        } catch (InterruptedException ie) {
            Thread.interrupted();
            throw new ClientException("Wait for settlement was interrupted", ie);
        } catch (ExecutionException exe) {
            throw ClientExceptionSupport.createNonFatalOrPassthrough(exe.getCause());
        } catch (TimeoutException te) {
            throw new ClientOperationTimedOutException("Timed out waiting for remote settlement", te);
        }
    }

    @Override
    public Tracker awaitAccepted() throws ClientException {
        try {
            if (settled() && !remoteSettled()) {
                return this;
            } else {
                settlementFuture().get();
                if (remoteState() != null && remoteState().isAccepted()) {
                    return this;
                } else {
                    throw new ClientDeliveryStateException("Remote did not accept the sent message", remoteState());
                }
            }
        } catch (ExecutionException exe) {
            throw ClientExceptionSupport.createNonFatalOrPassthrough(exe.getCause());
        } catch (InterruptedException ie) {
            Thread.interrupted();
            throw new ClientException("Wait for Accepted outcome was interrupted", ie);
        }
    }

    @Override
    public Tracker awaitAccepted(long timeout, TimeUnit unit) throws ClientException {
        try {
            if (settled() && !remoteSettled()) {
                return this;
            } else {
                settlementFuture().get(timeout, unit);
                if (remoteState() != null && remoteState().isAccepted()) {
                    return this;
                } else {
                    throw new ClientDeliveryStateException("Remote did not accept the sent message", remoteState());
                }
            }
        } catch (InterruptedException ie) {
            Thread.interrupted();
            throw new ClientException("Wait for Accepted outcome was interrupted", ie);
        } catch (ExecutionException exe) {
            throw ClientExceptionSupport.createNonFatalOrPassthrough(exe.getCause());
        } catch (TimeoutException te) {
            throw new ClientOperationTimedOutException("Timed out waiting for remote Accepted outcome", te);
        }
    }

    //----- Internal Event hooks for delivery updates

    private void processDeliveryUpdated(OutgoingDelivery delivery) {
        remotelySettled = delivery.isRemotelySettled();
        remoteDeliveryState = ClientDeliveryState.fromProtonType(delivery.getRemoteState());

        if (delivery.isRemotelySettled()) {
            remoteSettlementFuture.complete(this);
        }

        if (sender.options().autoSettle() && delivery.isRemotelySettled()) {
            delivery.settle();
        }
    }
}

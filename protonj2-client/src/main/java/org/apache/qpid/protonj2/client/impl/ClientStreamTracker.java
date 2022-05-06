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
import org.apache.qpid.protonj2.client.StreamSender;
import org.apache.qpid.protonj2.client.StreamTracker;
import org.apache.qpid.protonj2.client.exceptions.ClientDeliveryStateException;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientOperationTimedOutException;
import org.apache.qpid.protonj2.client.futures.ClientFuture;
import org.apache.qpid.protonj2.engine.OutgoingDelivery;

/**
 * {@link StreamTracker} implementation that relies on the ClientTracker to handle the
 * basic {@link OutgoingDelivery} management.
 */
public final class ClientStreamTracker implements StreamTracker {

    private final ClientStreamSender sender;
    private final OutgoingDelivery delivery;

    private final ClientFuture<StreamTracker> remoteSettlementFuture;

    private volatile boolean remotelySettled;
    private volatile DeliveryState remoteDeliveryState;

    ClientStreamTracker(ClientStreamSender sender, OutgoingDelivery delivery) {
        this.sender = sender;
        this.delivery = delivery;
        this.delivery.deliveryStateUpdatedHandler(this::processDeliveryUpdated);
        this.remoteSettlementFuture = sender.session().getFutureFactory().createFuture();
    }

    OutgoingDelivery delivery() {
        return delivery;
    }

    @Override
    public StreamSender sender() {
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
    public StreamTracker disposition(DeliveryState state, boolean settle) throws ClientException {
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
    public StreamTracker settle() throws ClientException {
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
    public ClientFuture<StreamTracker> settlementFuture() {
        if (delivery.isSettled()) {
            remoteSettlementFuture.complete(this);
        }

        return remoteSettlementFuture;
    }

    @Override
    public StreamTracker awaitSettlement() throws ClientException {
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
    public StreamTracker awaitSettlement(long timeout, TimeUnit unit) throws ClientException {
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
    public StreamTracker awaitAccepted() throws ClientException {
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
    public StreamTracker awaitAccepted(long timeout, TimeUnit unit) throws ClientException {
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

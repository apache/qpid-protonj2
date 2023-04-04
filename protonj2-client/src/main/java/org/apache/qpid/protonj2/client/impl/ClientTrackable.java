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

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.apache.qpid.protonj2.client.DeliveryState;
import org.apache.qpid.protonj2.client.exceptions.ClientDeliveryStateException;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientOperationTimedOutException;
import org.apache.qpid.protonj2.client.futures.ClientFuture;
import org.apache.qpid.protonj2.engine.OutgoingDelivery;

/**
 * Base type used to provide some common plumbing for Tracker types
 *
 * @param <SenderType> The client sender type that created this tracker
 * @param <TrackerType> The actual type of tracker that is being implemented
 */
public abstract class ClientTrackable<SenderType extends ClientSenderLinkType<?>, TrackerType> {

    protected final SenderType sender;
    protected final OutgoingDelivery delivery;

    @SuppressWarnings("rawtypes")
    protected static final AtomicIntegerFieldUpdater<ClientTrackable> REMOTELY_SETTLED_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(ClientTrackable.class, "remotelySettled");
    @SuppressWarnings("rawtypes")
    protected static final AtomicReferenceFieldUpdater<ClientTrackable, DeliveryState> REMOTEL_DELIVERY_STATE_UPDATER =
        AtomicReferenceFieldUpdater.newUpdater(ClientTrackable.class, DeliveryState.class, "remoteDeliveryState");

    private ClientFuture<TrackerType> remoteSettlementFuture;
    private volatile int remotelySettled;
    private volatile DeliveryState remoteDeliveryState;

    /**
     * Create an instance of a client outgoing delivery tracker.
     *
     * @param sender
     *      The sender that was used to send the delivery
     * @param delivery
     *      The proton outgoing delivery object that backs this tracker.
     */
    ClientTrackable(SenderType sender, OutgoingDelivery delivery) {
        Objects.requireNonNull(sender, "Sender cannot be null for a Tracker");

        this.sender = sender;
        this.delivery = delivery;
        this.delivery.deliveryStateUpdatedHandler(this::processDeliveryUpdated);
    }

    protected abstract TrackerType self();

    OutgoingDelivery delivery() {
        return delivery;
    }

    public synchronized DeliveryState state() {
        return ClientDeliveryState.fromProtonType(delivery.getState());
    }

    public DeliveryState remoteState() {
        return remoteDeliveryState;
    }

    public boolean remoteSettled() {
        return remotelySettled > 0;
    }

    public TrackerType disposition(DeliveryState state, boolean settle) throws ClientException {
        try {
            sender.disposition(delivery, ClientDeliveryState.asProtonType(state), settle);
        } finally {
            if (settle) {
                synchronized (this) {
                    if (remoteSettlementFuture == null) {
                        remoteSettlementFuture = sender.session.connection().getFutureFactory().createFuture();
                    }
                    remoteSettlementFuture.complete(self());
                }
            }
        }

        return self();
    }

    public TrackerType settle() throws ClientException {
        try {
            sender.disposition(delivery, null, true);
        } finally {
            synchronized (this) {
                if (remoteSettlementFuture == null) {
                    remoteSettlementFuture = sender.session.connection().getFutureFactory().createFuture();
                }
                remoteSettlementFuture.complete(self());
            }
        }

        return self();
    }

    public synchronized boolean settled() {
        return delivery.isSettled();
    }

    public ClientFuture<TrackerType> settlementFuture() {
        synchronized (this) {
            if (remoteSettlementFuture == null) {
                remoteSettlementFuture = sender.session.connection().getFutureFactory().createFuture();
            }

            if (delivery.isSettled() || remoteSettled()) {
                remoteSettlementFuture.complete(self());
            }
        }

        return remoteSettlementFuture;
    }

    public TrackerType awaitSettlement() throws ClientException {
        try {
            if (settled()) {
                return self();
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

    public TrackerType awaitSettlement(long timeout, TimeUnit unit) throws ClientException {
        try {
            if (settled()) {
                return self();
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

    public TrackerType awaitAccepted() throws ClientException {
        try {
            if (settled() && !remoteSettled()) {
                return self();
            } else {
                settlementFuture().get();
                if (remoteState() != null && remoteState().isAccepted()) {
                    return self();
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

    public TrackerType awaitAccepted(long timeout, TimeUnit unit) throws ClientException {
        try {
            if (settled() && !remoteSettled()) {
                return self();
            } else {
                settlementFuture().get(timeout, unit);
                if (remoteState() != null && remoteState().isAccepted()) {
                    return self();
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
        if (delivery.isRemotelySettled()) {
            synchronized (this) {
                REMOTEL_DELIVERY_STATE_UPDATER.lazySet(this, ClientDeliveryState.fromProtonType(delivery.getRemoteState()));
                REMOTELY_SETTLED_UPDATER.lazySet(this, 1);

                if (remoteSettlementFuture != null) {
                    remoteSettlementFuture.complete(self());
                }
            }

            if (sender.options.autoSettle()) {
                delivery.settle();
            }
        } else {
            REMOTEL_DELIVERY_STATE_UPDATER.set(this, ClientDeliveryState.fromProtonType(delivery.getRemoteState()));
        }
    }
}

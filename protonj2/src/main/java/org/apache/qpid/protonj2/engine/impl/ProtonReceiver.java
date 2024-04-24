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
package org.apache.qpid.protonj2.engine.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.engine.EventHandler;
import org.apache.qpid.protonj2.engine.IncomingDelivery;
import org.apache.qpid.protonj2.engine.LinkCreditState;
import org.apache.qpid.protonj2.engine.Receiver;
import org.apache.qpid.protonj2.engine.Session;
import org.apache.qpid.protonj2.engine.exceptions.ProtocolViolationException;
import org.apache.qpid.protonj2.engine.util.DeliveryIdTracker;
import org.apache.qpid.protonj2.engine.util.UnsettledMap;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.transport.Attach;
import org.apache.qpid.protonj2.types.transport.DeliveryState;
import org.apache.qpid.protonj2.types.transport.Detach;
import org.apache.qpid.protonj2.types.transport.Disposition;
import org.apache.qpid.protonj2.types.transport.Flow;
import org.apache.qpid.protonj2.types.transport.Role;
import org.apache.qpid.protonj2.types.transport.Transfer;

/**
 * Proton Receiver link implementation.
 */
public class ProtonReceiver extends ProtonLink<Receiver> implements Receiver {

    private EventHandler<IncomingDelivery> deliveryReadEventHandler = null;
    private EventHandler<IncomingDelivery> deliveryAbortedEventHandler = null;
    private EventHandler<IncomingDelivery> deliveryUpdatedEventHandler = null;
    private EventHandler<Receiver> linkCreditUpdatedHandler = null;

    private final ProtonSessionIncomingWindow sessionWindow;
    private final DeliveryIdTracker currentDeliveryId = new DeliveryIdTracker();
    private final UnsettledMap<ProtonIncomingDelivery> unsettled =
        new UnsettledMap<ProtonIncomingDelivery>(ProtonIncomingDelivery::getDeliveryIdInt);

    private DeliveryState defaultDeliveryState;
    private LinkCreditState drainStateSnapshot;

    /**
     * Create a new {@link Receiver} instance with the given {@link Session} parent.
     *
     *  @param session
     *      The Session that is linked to this receiver instance.
     *  @param name
     *      The name assigned to this {@link Receiver} link.
     */
    public ProtonReceiver(ProtonSession session, String name) {
        super(session, name, new ProtonLinkCreditState());

        this.sessionWindow = session.getIncomingWindow();
    }

    @Override
    public ProtonReceiver setDefaultDeliveryState(DeliveryState state) {
        this.defaultDeliveryState = state;
        return this;
    }

    @Override
    public DeliveryState getDefaultDeliveryState() {
        return defaultDeliveryState;
    }

    @Override
    public Role getRole() {
        return Role.RECEIVER;
    }

    @Override
    protected ProtonReceiver self() {
        return this;
    }

    @Override
    public int getCredit() {
        return getCreditState().getCredit();
    }

    @Override
    public ProtonReceiver addCredit(int credit) {
        checkLinkOperable("Cannot add credit");

        if (credit < 0) {
            throw new IllegalArgumentException("additional credits cannot be less than zero");
        }

        if (credit > 0) {
            getCreditState().incrementCredit(credit);
            if (isLocallyOpen() && wasLocalAttachSent()) {
                sessionWindow.writeFlow(this);
            }
        }

        return this;
    }

    @Override
    public boolean drain() {
        checkLinkOperable("Cannot drain Receiver");

        if (drainStateSnapshot != null) {
            throw new IllegalStateException("Drain attempt already outstanding");
        }

        if (getCredit() > 0) {
            drainStateSnapshot = getCreditState().snapshot();

            if (isLocallyOpen() && wasLocalAttachSent()) {
                sessionWindow.writeFlow(this);
            }
        }

        return isDraining();
    }

    @Override
    public boolean drain(int credits) {
        checkLinkOperable("Cannot drain Receiver");

        if (drainStateSnapshot != null) {
            throw new IllegalStateException("Drain attempt already outstanding");
        }

        final int currentCredit = getCredit();

        if (credits < 0) {
            throw new IllegalArgumentException("Cannot drain negative link credit");
        }

        if (credits < currentCredit) {
            throw new IllegalArgumentException("Cannot drain partial link credit");
        }

        getCreditState().incrementCredit(credits - currentCredit);

        if (getCredit() > 0) {
            drainStateSnapshot = getCreditState().snapshot();

            if (isLocallyOpen() && wasLocalAttachSent()) {
                sessionWindow.writeFlow(this);
            }
        }

        return isDraining();
    }

    @Override
    public boolean isDraining() {
        return drainStateSnapshot != null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Receiver disposition(Predicate<IncomingDelivery> filter, DeliveryState disposition, boolean settle) {
        checkLinkOperable("Cannot apply disposition");
        Objects.requireNonNull(filter, "Supplied filter cannot be null");

        List<UnsignedInteger> toRemove = settle ? new ArrayList<>() : Collections.EMPTY_LIST;

        unsettled.forEach((deliveryId, delivery) -> {
            if (filter.test(delivery)) {
                if (disposition != null) {
                    delivery.localState(disposition);
                }
                if (settle) {
                    delivery.locallySettled();
                    toRemove.add(deliveryId);
                }
                sessionWindow.processDisposition(this, delivery);
            }
        });

        if (!toRemove.isEmpty()) {
            toRemove.forEach(deliveryId -> unsettled.remove(deliveryId));
        }

        return this;
    }

    @Override
    public Receiver settle(Predicate<IncomingDelivery> filter) {
        return disposition(filter, null, true);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Collection<IncomingDelivery> unsettled() {
        if (unsettled.isEmpty()) {
            return Collections.EMPTY_LIST;
        } else {
            return Collections.unmodifiableCollection(new ArrayList<>(unsettled.values()));
        }
    }

    @Override
    public boolean hasUnsettled() {
        return !unsettled.isEmpty();
    }

    //----- Delivery related access points

    void disposition(ProtonIncomingDelivery delivery) {
        if (!delivery.isRemotelySettled()) {
            checkLinkOperable("Cannot set a disposition for delivery");
        }

        try {
            sessionWindow.processDisposition(this, delivery);
        } finally {
            if (delivery.isSettled()) {
                unsettled.remove((int) delivery.getDeliveryId());
                if (delivery.getTag() != null) {
                    delivery.getTag().release();
                }
            }
        }
    }

    void deliveryRead(ProtonIncomingDelivery delivery, int bytesRead) {
        if (areDeliveriesStillActive()) {
            sessionWindow.deliveryRead(delivery, bytesRead);
        }
    }

    //----- Receiver event handlers

    @Override
    public Receiver deliveryReadHandler(EventHandler<IncomingDelivery> handler) {
        this.deliveryReadEventHandler = handler;
        return this;
    }

    Receiver signalDeliveryRead(ProtonIncomingDelivery delivery) {
        if (delivery.deliveryReadHandler() != null) {
            delivery.deliveryReadHandler().handle(delivery);
        } else if (deliveryReadEventHandler != null) {
            deliveryReadEventHandler.handle(delivery);
        }

        // Allow session owner to monitor deliveries passing through the session
        // but only after the receiver handlers have had a chance to handle it.
        if (session.deliveryReadHandler() != null) {
            session.deliveryReadHandler().handle(delivery);
        }

        return this;
    }

    @Override
    public Receiver deliveryAbortedHandler(EventHandler<IncomingDelivery> handler) {
        this.deliveryAbortedEventHandler = handler;
        return this;
    }

    Receiver signalDeliveryAborted(ProtonIncomingDelivery delivery) {
        if (delivery.deliveryAbortedHandler() != null) {
            delivery.deliveryAbortedHandler().handle(delivery);
        } else if (delivery.deliveryReadHandler() != null) {
            delivery.deliveryReadHandler().handle(delivery);
        } else if (deliveryAbortedEventHandler != null) {
            deliveryAbortedEventHandler.handle(delivery);
        } else if (deliveryReadEventHandler != null) {
            deliveryReadEventHandler.handle(delivery);
        }

        return this;
    }

    @Override
    public Receiver deliveryStateUpdatedHandler(EventHandler<IncomingDelivery> handler) {
        this.deliveryUpdatedEventHandler = handler;
        return this;
    }

    Receiver signalDeliveryStateUpdated(ProtonIncomingDelivery delivery) {
        if (delivery.deliveryStateUpdatedHandler() != null) {
            delivery.deliveryStateUpdatedHandler().handle(delivery);
        } else if (deliveryUpdatedEventHandler != null) {
            deliveryUpdatedEventHandler.handle(delivery);
        }

        return this;
    }

    @Override
    public Receiver creditStateUpdateHandler(EventHandler<Receiver> handler) {
        this.linkCreditUpdatedHandler = handler;
        return this;
    }

    Receiver signalLinkCreditStateUpdated() {
        if (linkCreditUpdatedHandler != null) {
            linkCreditUpdatedHandler.handle(this);
        }

        return this;
    }

    //----- Handle incoming frames from the remote sender

    @Override
    protected final ProtonReceiver handleRemoteAttach(Attach attach) {
        if (!attach.hasInitialDeliveryCount()) {
            throw new ProtocolViolationException("Sending peer attach had no initial delivery count");
        }

        getCreditState().initializeDeliveryCount((int) attach.getInitialDeliveryCount());

        return this;
    }

    @Override
    protected final ProtonReceiver handleRemoteDetach(Detach detach) {
        return this;
    }

    @Override
    protected final ProtonReceiver handleRemoteFlow(Flow flow) {
        ProtonLinkCreditState creditState = getCreditState();
        creditState.remoteFlow(flow);

        if (flow.getDrain()) {
            creditState.updateDeliveryCount((int) flow.getDeliveryCount());
            creditState.updateCredit((int) flow.getLinkCredit());
            if (creditState.getCredit() != 0) {
                throw new IllegalArgumentException("Receiver read flow with drain set but credit was not zero");
            } else {
                drainStateSnapshot = null;
            }
        }

        signalLinkCreditStateUpdated();

        return this;
    }

    @Override
    protected final ProtonReceiver handleRemoteDisposition(Disposition disposition, ProtonIncomingDelivery delivery) {
        boolean updated = false;

        if (disposition.getState() != null && !disposition.getState().equals(delivery.getRemoteState())) {
            updated = true;
            delivery.remoteState(disposition.getState());
        }

        if (disposition.getSettled() && !delivery.isRemotelySettled()) {
            updated = true;
            delivery.remotelySettled();
        }

        if (updated) {
            signalDeliveryStateUpdated(delivery);
        }

        return this;
    }

    @Override
    protected final ProtonReceiver handleRemoteDisposition(Disposition disposition, ProtonOutgoingDelivery delivery) {
        throw new IllegalStateException("Receiver link should never handle dispositions for outgoing deliveries");
    }

    @Override
    protected final ProtonIncomingDelivery handleRemoteTransfer(Transfer transfer, ProtonBuffer payload) {
        final ProtonIncomingDelivery delivery;

        if (!currentDeliveryId.isEmpty() && (!transfer.hasDeliveryId() || currentDeliveryId.equals((int) transfer.getDeliveryId()))) {
            delivery = unsettled.get(currentDeliveryId.intValue());
        } else {
            verifyNewDeliveryIdSequence(transfer, currentDeliveryId);

            delivery = new ProtonIncomingDelivery(this, transfer.getDeliveryId(), transfer.getDeliveryTag());
            delivery.setMessageFormat((int) transfer.getMessageFormat());

            unsettled.put((int) transfer.getDeliveryId(), delivery);
            currentDeliveryId.set((int) transfer.getDeliveryId());
        }

        if (transfer.hasState()) {
            delivery.remoteState(transfer.getState());
        }

        if (transfer.getSettled() || transfer.getAborted()) {
            delivery.remotelySettled();
        }

        if (payload != null) {
            delivery.appendTransferPayload(payload);
        }

        final boolean done = transfer.getAborted() || !transfer.getMore();
        if (done) {
            getCreditState().decrementCredit();
            getCreditState().incrementDeliveryCount();
            currentDeliveryId.reset();

            if (transfer.getAborted()) {
                delivery.aborted();
            } else {
                delivery.completed();
            }
        }

        if (transfer.getAborted()) {
            signalDeliveryAborted(delivery);
        } else {
            signalDeliveryRead(delivery);
        }

        if (isDraining() && getCredit() == 0) {
            drainStateSnapshot = null;
            signalLinkCreditStateUpdated();
        }

        return delivery;
    }

    @Override
    protected ProtonReceiver decorateOutgoingFlow(Flow flow) {
        flow.setLinkCredit(getCredit());
        flow.setHandle(getHandle());
        if (getCreditState().isDeliveryCountInitialized()) {
            flow.setDeliveryCount(getCreditState().getDeliveryCount());
        }
        flow.setDrain(isDraining());
        if (getCreditState().isEcho()) {
            flow.setEcho(true);
        }

        return this;
    }

    private void verifyNewDeliveryIdSequence(Transfer transfer, DeliveryIdTracker currentDeliveryId) {
        if (!transfer.hasDeliveryId()) {
            getEngine().engineFailed(
                new ProtocolViolationException("No delivery-id specified on first Transfer of new delivery"));
        }

        sessionWindow.validateNextDeliveryId(transfer.getDeliveryId());

        if (!currentDeliveryId.isEmpty()) {
            getEngine().engineFailed(
                new ProtocolViolationException("Illegal multiplex of deliveries on same link with delivery-id " +
                                               currentDeliveryId + " and " + transfer.getDeliveryId()));
        }
    }
}

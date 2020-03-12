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
package org.apache.qpid.proton4j.engine.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.transport.Attach;
import org.apache.qpid.proton4j.amqp.transport.DeliveryState;
import org.apache.qpid.proton4j.amqp.transport.Detach;
import org.apache.qpid.proton4j.amqp.transport.Disposition;
import org.apache.qpid.proton4j.amqp.transport.Flow;
import org.apache.qpid.proton4j.amqp.transport.Role;
import org.apache.qpid.proton4j.amqp.transport.Transfer;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.engine.EventHandler;
import org.apache.qpid.proton4j.engine.LinkCreditState;
import org.apache.qpid.proton4j.engine.OutgoingDelivery;
import org.apache.qpid.proton4j.engine.Sender;
import org.apache.qpid.proton4j.engine.Session;
import org.apache.qpid.proton4j.engine.util.DeliveryIdTracker;
import org.apache.qpid.proton4j.engine.util.SplayMap;

/**
 * Proton Sender link implementation.
 */
public class ProtonSender extends ProtonLink<Sender> implements Sender {

    private final ProtonSessionOutgoingWindow sessionWindow;

    private final DeliveryIdTracker currentDelivery = new DeliveryIdTracker();

    private boolean sendable;

    private final SplayMap<ProtonOutgoingDelivery> unsettled = new SplayMap<>();

    private EventHandler<OutgoingDelivery> deliveryUpdatedEventHandler = null;
    private EventHandler<Sender> sendableEventHandler = null;
    private EventHandler<Sender> linkCreditUpdatedHandler = null;
    private EventHandler<LinkCreditState> drainRequestedEventHandler = null;

    private OutgoingDelivery current;

    /**
     * Create a new {@link Sender} instance with the given {@link Session} parent.
     *
     *  @param session
     *      The Session that is linked to this sender instance.
     *  @param name
     *      The name assigned to this {@link Sender} link.
     */
    public ProtonSender(ProtonSession session, String name) {
        super(session, name, new ProtonLinkCreditState(0));

        this.sessionWindow = session.getOutgoingWindow();
    }

    @Override
    public Role getRole() {
        return Role.SENDER;
    }

    @Override
    protected ProtonSender self() {
        return this;
    }

    @Override
    public int getCredit() {
        return getCreditState().getCredit();
    }

    @Override
    public boolean isSendable() {
        return sendable;
    }

    @Override
    public boolean isDraining() {
        return getCreditState().isDrain();
    }

    @Override
    public Sender drained() {
        checkLinkOperable("Cannot report link drained.");

        final ProtonLinkCreditState state = getCreditState();

        if (state.isDrain() && state.hasCredit()) {
            int drained = state.getCredit();

            state.clearCredit();
            state.incrementDeliveryCount(drained);

            session.writeFlow(this);

            state.clearDrain();
        }

        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Sender disposition(Predicate<OutgoingDelivery> filter, DeliveryState state, boolean settle) {
        checkLinkOperable("Cannot apply disposition");

        List<UnsignedInteger> toRemove = settle ? new ArrayList<>() : Collections.EMPTY_LIST;

        unsettled.forEach((deliveryId, delivery) -> {
            if (filter.test(delivery)) {
                if (filter != null) {
                    delivery.localState(state);
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
    public Sender settle(Predicate<OutgoingDelivery> filter) {
        disposition(filter, null, true);
        return this;
    }

    @Override
    public OutgoingDelivery current() {
        return current;
    }

    @Override
    public OutgoingDelivery next() {
        checkLinkOperable("Cannot update next delivery");

        if (current != null) {
            throw new IllegalStateException("Current delivery is not complete and cannot be advanced.");
        }

        return current = new ProtonOutgoingDelivery(this);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Collection<OutgoingDelivery> unsettled() {
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

    //----- Handle remote events for this Sender

    @Override
    protected final ProtonSender handleRemoteAttach(Attach attach) {
        return this;
    }

    @Override
    protected final ProtonSender handleRemoteDetach(Detach detach) {
        return this;
    }

    @Override
    protected final ProtonSender handleRemoteDisposition(Disposition disposition, ProtonIncomingDelivery delivery) {
        throw new IllegalStateException("Sender link should never handle dispsotiions for incoming deliveries");
    }

    @Override
    protected final ProtonSender handleRemoteDisposition(Disposition disposition, ProtonOutgoingDelivery delivery) {
        boolean updated = false;

        if (disposition.getState() != null && !disposition.getState().equals(delivery.getRemoteState())) {
            updated = true;
            delivery.remoteState(disposition.getState());
        }

        if (disposition.getSettled() && !delivery.isRemotelySettled()) {
            updated = true;
            // TODO - Casting is ugly but right now our unsigned integers are longs
            unsettled.remove((int) delivery.getDeliveryId());
            delivery.remotelySettled();
        }

        if (updated) {
            delivery.getLink().signalDeliveryUpdated(delivery);
        }

        return this;
    }

    @Override
    protected final ProtonIncomingDelivery handleRemoteTransfer(Transfer transfer, ProtonBuffer payload) {
        throw new IllegalArgumentException("Sender end cannot process incoming transfers");
    }

    @Override
    protected final ProtonSender handleRemoteFlow(Flow flow) {
        ProtonLinkCreditState creditState = getCreditState();

        creditState.remoteFlow(flow);

        int existingDeliveryCount = creditState.getDeliveryCount();
        // int casts are expected, credit is a uint and delivery-count is really a uint sequence which wraps, so we
        // just use the truncation and overflows.  Receivers flow might not have any delivery-count, as sender initializes
        // on attach! We initialize to 0 so we can just ignore that.
        int remoteDeliveryCount = (int) flow.getDeliveryCount();
        int newDeliveryCountLimit = remoteDeliveryCount + (int) flow.getLinkCredit();

        long effectiveCredit = 0xFFFFFFFFL & newDeliveryCountLimit - existingDeliveryCount;
        if (effectiveCredit > 0) {
            creditState.updateCredit((int) effectiveCredit);
        } else {
            creditState.updateCredit(0);
        }

        if (isLocallyOpen()) {
            if (getCredit() > 0 && !sendable) {
                sendable = true;
                signalSendable();
            }

            if (flow.getDrain() && getCredit() > 0) {
                signalDrainRequested();
            }

            signalLinkCreditUpdated();
        }

        return this;
    }

    @Override
    protected final ProtonSender decorateOutgoingFlow(Flow flow) {
        flow.setLinkCredit(getCredit());
        flow.setHandle(getHandle());
        flow.setDeliveryCount(getCreditState().getDeliveryCount());
        flow.setDrain(isDraining());

        return this;
    }

    //----- Delivery output related access points

    void send(ProtonOutgoingDelivery delivery, ProtonBuffer buffer) {
        if (!isSendable()) {
            checkLinkOperable("Send failed due to link state");

            throw new IllegalStateException("Cannot send when sender has no capacity to do so.");
        }

        if (currentDelivery.isEmpty()) {
            currentDelivery.set(sessionWindow.getAndIncrementNextDeliveryId());

            delivery.setDeliveryId(currentDelivery.longValue());
        }

        if (!delivery.isSettled()) {
            // TODO - Casting is ugly but right now our unsigned integers are longs
            unsettled.put((int) delivery.getDeliveryId(), delivery);
        }

        if (!delivery.isPartial()) {
            currentDelivery.reset();
            current = null;
            getCreditState().incrementDeliveryCount();
            getCreditState().decrementCredit();

            if (getCredit() == 0) {
                sendable = false;
                getCreditState().clearDrain();
            }
        }

        sessionWindow.processSend(this, delivery, buffer);
    }

    void disposition(ProtonOutgoingDelivery delivery) {
        checkLinkOperable("Cannot set a disposition");

        if (delivery.isSettled()) {
            // TODO - Casting is ugly but right now our unsigned integers are longs
            unsettled.remove((int) delivery.getDeliveryId());
            if (delivery.getTag() != null) {
                delivery.getTag().release();
            }
        }

        sessionWindow.processDisposition(this, delivery);
    }

    void abort(ProtonOutgoingDelivery delivery) {
        checkLinkOperable("Cannot abort Transfer");

        // Clean up delivery related resources and then fire off the abort transfer
        unsettled.remove((int) delivery.getDeliveryId());
        currentDelivery.reset();
        current = null;

        sessionWindow.processAbort(this, delivery);
    }

    //----- Sender event handlers

    @Override
    public Sender linkCreditUpdateHandler(EventHandler<Sender> handler) {
        this.linkCreditUpdatedHandler = handler;
        return this;
    }

    Sender signalLinkCreditUpdated() {
        if (linkCreditUpdatedHandler != null) {
            linkCreditUpdatedHandler.handle(this);
        }

        return this;
    }

    @Override
    public Sender deliveryUpdatedHandler(EventHandler<OutgoingDelivery> handler) {
        this.deliveryUpdatedEventHandler = handler;
        return this;
    }

    Sender signalDeliveryUpdated(OutgoingDelivery delivery) {
        if (deliveryUpdatedEventHandler != null) {
            deliveryUpdatedEventHandler.handle(delivery);
        }
        return this;
    }

    @Override
    public Sender sendableHandler(EventHandler<Sender> handler) {
        this.sendableEventHandler = handler;
        return this;
    }

    Sender signalSendable() {
        if (sendableEventHandler != null) {
            sendableEventHandler.handle(this);
        }
        return this;
    }

    Sender signalNoLongerSendable() {
        sendable = false;
        return this;
    }

    @Override
    public Sender drainRequestedHandler(EventHandler<LinkCreditState> handler) {
        this.drainRequestedEventHandler = handler;
        return this;
    }

    Sender signalDrainRequested() {
        // TODO - The intention is to snapshot credit state here so that on drained we can properly
        //        reduce link credit in case the remote has updated the credit since the event was
        //        triggered.
        if (drainRequestedEventHandler != null) {
            drainRequestedEventHandler.handle(getCreditState().snapshot());
        }
        return this;
    }

    //----- Internal routing and state management

    @Override
    protected void transitionedToLocallyOpened() {
        localAttach.setInitialDeliveryCount(currentDelivery.longValue());
        if (getCredit() > 0) {
            sendable = true;
        }
    }

    @Override
    protected void transitionedToLocallyDetached() {
        sendable = false;
    }

    @Override
    protected void transitionedToLocallyClosed() {
        sendable = false;
    }

    @Override
    protected void transitionToRemotelyOpenedState() {
        // Nothing to do yet on remotely opened.
    }

    @Override
    protected void transitionToRemotelyDetached() {
        sendable = false;
    }

    @Override
    protected void transitionToRemotelyCosed() {
        sendable = false;
    }

    @Override
    protected void transitionToParentLocallyClosed() {
        sendable = false;
    }

    @Override
    protected void transitionToParentRemotelyClosed() {
        sendable = false;
    }
}

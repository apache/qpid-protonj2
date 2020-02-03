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
import org.apache.qpid.proton4j.engine.IncomingDelivery;
import org.apache.qpid.proton4j.engine.LinkCreditState;
import org.apache.qpid.proton4j.engine.Receiver;
import org.apache.qpid.proton4j.engine.Session;
import org.apache.qpid.proton4j.engine.exceptions.ProtocolViolationException;
import org.apache.qpid.proton4j.engine.util.DeliveryIdTracker;
import org.apache.qpid.proton4j.engine.util.SplayMap;

/**
 * Proton Receiver link implementation.
 */
public class ProtonReceiver extends ProtonLink<Receiver> implements Receiver {

    private EventHandler<IncomingDelivery> deliveryReceivedEventHandler = null;
    private EventHandler<IncomingDelivery> deliveryUpdatedEventHandler = null;
    private EventHandler<Receiver> receiverDrainedEventHandler = null;

    private final ProtonSessionIncomingWindow sessionWindow;
    private final ProtonLinkCreditState creditState = new ProtonLinkCreditState();
    private final DeliveryIdTracker currentDeliveryId = new DeliveryIdTracker();
    private final SplayMap<ProtonIncomingDelivery> unsettled = new SplayMap<>();

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
        super(session, name);

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
        return creditState.getCredit();
    }

    @Override
    public ProtonReceiver addCredit(int credit) {
        checkNotClosed("Cannot add credit on a closed Receiver");

        // TODO - Better way to check all this state on each operation.
        //        One possible way of doing this is by having a Consumer<> type and
        //        swap the method when closed to one that always throws.
        if (session.isLocallyClosed() || connection.isLocallyClosed() ||
            session.isRemotelyClosed() || connection.isRemotelyClosed()) {
            throw new IllegalStateException("Cannot set credit when session or connection already closed");
        }

        if (credit < 0) {
            throw new IllegalArgumentException("additional credits cannot be less than zero");
        }

        if (credit > 0) {
            creditState.incrementCredit(credit);
            if (isRemotelyOpen()) {
                // TODO: delaying credit until remoteAttach(Attach attach) is called doesn't seem needed?
                // Perhaps should be/include isLocallyOpen?
                sessionWindow.writeFlow(this);
            }
        }

        return this;
    }

    @Override
    public Receiver drain() {
        checkNotClosed("Cannot drain a closed Receiver");

        if (drainStateSnapshot != null) {
            throw new IllegalStateException("Drain attempt already outstanding");
        }

        LinkCreditState snapshot = creditState.snapshot();
        if (snapshot.getCredit() <= 0) {
            throw new IllegalStateException("No existing credit to drain");
        }

        drainStateSnapshot = snapshot;

        if (isRemotelyOpen()) {
            // TODO: delaying credit+drain until remoteAttach(Attach attach) is called doesn't seem needed?
            sessionWindow.writeFlow(this);
        }

        return this;
    }

    @Override
    public boolean isDrain() {
        return drainStateSnapshot != null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Receiver disposition(Predicate<IncomingDelivery> filter, DeliveryState disposition, boolean settle) {
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

    //----- Delivery related access points

    void disposition(ProtonIncomingDelivery delivery) {
        checkNotClosed("Cannot set a disposition for a delivery on a closed Receiver");

        // TODO - Better way to check all this state on each operation.
        if (session.isLocallyClosed() || connection.isLocallyClosed() ||
            session.isRemotelyClosed() || connection.isRemotelyClosed()) {
            throw new IllegalStateException("Cannot set credit when session or connection already closed");
        }

        // TODO - Enforce not closed etc
        if (delivery.isSettled()) {
            // TODO - Casting is ugly but right now our unsigned integers are longs
            unsettled.remove((int) delivery.getDeliveryId());
        }

        sessionWindow.processDisposition(this, delivery);
    }

    void deliveryRead(ProtonIncomingDelivery delivery, int bytesRead) {
        // TODO - When any resource is closed we could still allow read of inbound data but the user
        //        can't operate on the delivery so do we want that ?

        sessionWindow.deliveryRead(delivery, bytesRead);
    }

    //----- Receiver event handlers

    @Override
    public Receiver deliveryReceivedHandler(EventHandler<IncomingDelivery> handler) {
        this.deliveryReceivedEventHandler = handler;
        return this;
    }

    Receiver signalDeliveryReceived(IncomingDelivery delivery) {
        //TODO: what if it is null? Limbo? Release? Should we instead error out?
        if (deliveryReceivedEventHandler != null) {
            deliveryReceivedEventHandler.handle(delivery);
        }
        return this;
    }

    @Override
    public Receiver deliveryUpdatedHandler(EventHandler<IncomingDelivery> handler) {
        this.deliveryUpdatedEventHandler = handler;
        return this;
    }

    Receiver signalDeliveryUpdated(IncomingDelivery delivery) {
        if (deliveryUpdatedEventHandler != null) {
            deliveryUpdatedEventHandler.handle(delivery);
        }
        return this;
    }

    @Override
    public Receiver drainStateUpdatedHandler(EventHandler<Receiver> handler) {
        this.receiverDrainedEventHandler = handler;
        return this;
    }

    Receiver signalReceiverDrained() {
        drainStateSnapshot = null;
        if (receiverDrainedEventHandler != null) {
            receiverDrainedEventHandler.handle(this);
        }
        return this;
    }

    //----- Handle link and parent resource state changes

    @Override
    protected void transitionedToLocallyOpened() {
        // Nothing currently updated on this state change.
    }

    @Override
    protected void transitionedToLocallyDetached() {
        creditState.clearCredit();
    }

    @Override
    protected void transitionedToLocallyClosed() {
        creditState.clearCredit();
    }

    @Override
    protected void transitionToRemotelyOpenedState() {
        // Nothing currently updated on this state change.
    }

    @Override
    protected void transitionToRemotelyDetachedState() {
        creditState.clearCredit();
    }

    @Override
    protected void transitionToRemotelyCosedState() {
        creditState.clearCredit();
    }

    @Override
    protected void transitionToParentLocallyClosedState() {
        creditState.clearCredit();
    }

    @Override
    protected void transitionToParentRemotelyClosedState() {
        creditState.clearCredit();
    }

    //----- Handle incoming frames from the remote sender

    @Override
    protected final ProtonReceiver handleRemoteAttach(Attach attach) {
        if (!attach.hasInitialDeliveryCount()) {
            //TODO: nicer handling of the error
            throw new IllegalArgumentException("Sending peer attach had no initial delivery count");
        }

        creditState.initialiseDeliveryCount((int) attach.getInitialDeliveryCount());

        if (getCredit() > 0 && isLocallyOpen()) {
            sessionWindow.writeFlow(this);
        }

        return this;
    }

    @Override
    protected final ProtonReceiver handleRemoteDetach(Detach detach) {
        creditState.clearCredit();
        return this;
    }

    @Override
    protected final ProtonReceiver handleRemoteFlow(Flow flow) {
        creditState.remoteFlow(flow);

        if (flow.getDrain()) {
            creditState.updateDeliveryCount((int) flow.getDeliveryCount());
            creditState.updateCredit((int) flow.getLinkCredit());
            if (creditState.getCredit() != 0) {
                throw new IllegalArgumentException("Receiver read flow with drain set but credit was not zero");
            }

            // TODO - engine error on credit being non-zero for drain response ?

            signalReceiverDrained();
        }

        //TODO: else somehow notify of remote flow? (e.g session windows changed, peer echo'd its view of the state
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
    protected final ProtonReceiver handleRemoteDisposition(Disposition disposition, ProtonOutgoingDelivery delivery) {
        throw new IllegalStateException("Receiver link should never handle dispsotiions for outgoing deliveries");
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

            // TODO - Casting is ugly but our ID values are longs
            unsettled.put((int) transfer.getDeliveryId(), delivery);
            currentDeliveryId.set((int) transfer.getDeliveryId());
        }

        if (transfer.hasState()) {
            delivery.remoteState(transfer.getState());
        }

        if (transfer.getSettled() || transfer.getAborted()) {
            delivery.remotelySettled();
        }

        delivery.appendTransferPayload(payload);

        boolean done = transfer.getAborted() || !transfer.getMore();
        if (done) {
            if (transfer.getAborted()) {
                delivery.aborted();
            } else {
                delivery.completed();
            }

            creditState.decrementCredit();
            creditState.incrementDeliveryCount();
            currentDeliveryId.reset();
        }

        if (delivery.isFirstTransfer()) {
            signalDeliveryReceived(delivery);
        } else {
            signalDeliveryUpdated(delivery);
        }

        return delivery;
    }

    @Override
    protected ProtonReceiver decorateOutgoingFlow(Flow flow) {
        flow.setLinkCredit(getCredit());
        flow.setHandle(getHandle());
        if (creditState.isDeliveryCountInitalised()) {
            flow.setDeliveryCount(creditState.getDeliveryCount());
        }
        flow.setDrain(isDrain());

        return this;
    }

    private void verifyNewDeliveryIdSequence(Transfer transfer, DeliveryIdTracker currentDeliveryId) {
        // TODO - Fail engine, session, or link ?

        if (!transfer.hasDeliveryId()) {
            getSession().getEngine().engineFailed(
                 new ProtocolViolationException("No delivery-id specified on first Transfer of new delivery"));
        }

        sessionWindow.validateNextDeliveryId(transfer.getDeliveryId());

        if (!currentDeliveryId.isEmpty()) {
            getSession().getEngine().engineFailed(
                new ProtocolViolationException("Illegal multiplex of deliveries on same link with delivery-id " +
                                               currentDeliveryId + " and " + transfer.getDeliveryId()));
        }
    }
}

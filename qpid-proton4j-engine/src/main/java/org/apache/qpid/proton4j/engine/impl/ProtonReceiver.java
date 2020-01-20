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
import java.util.function.Predicate;

import org.apache.qpid.proton4j.amqp.transport.DeliveryState;
import org.apache.qpid.proton4j.amqp.transport.Disposition;
import org.apache.qpid.proton4j.amqp.transport.Role;
import org.apache.qpid.proton4j.engine.EventHandler;
import org.apache.qpid.proton4j.engine.IncomingDelivery;
import org.apache.qpid.proton4j.engine.LinkCreditState;
import org.apache.qpid.proton4j.engine.Receiver;
import org.apache.qpid.proton4j.engine.Session;

/**
 * Proton Receiver link implementation.
 */
public class ProtonReceiver extends ProtonLink<Receiver> implements Receiver {

    private final ProtonReceiverState linkState;

    private EventHandler<IncomingDelivery> deliveryReceivedEventHandler = null;
    private EventHandler<IncomingDelivery> deliveryUpdatedEventHandler = null;
    private EventHandler<Receiver> receiverDrainedEventHandler = null;

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
        this.linkState = new ProtonReceiverState(this, session.getIncomingWindow());
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
    protected ProtonReceiverState linkState() {
        return linkState;
    }

    @Override
    public int getCredit() {
        return linkState.getCredit();
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

        linkState.addCredit(credit);

        return this;
    }

    @Override
    public Receiver drain() {
        checkNotClosed("Cannot drain a closed Receiver");

        if (drainStateSnapshot != null) {
            throw new IllegalStateException("Drain attempt already outstanding");
        }

        LinkCreditState snapshot = linkState.snapshotCreditState();
        if (snapshot.getCredit() <= 0) {
            throw new IllegalStateException("No existing credit to drain");
        }

        drainStateSnapshot = snapshot;

        linkState.drain();

        return this;
    }

    @Override
    public boolean isDrain() {
        return drainStateSnapshot != null;
    }

    @Override
    public Receiver disposition(Predicate<IncomingDelivery> filter, DeliveryState state, boolean settle) {
        linkState.applyDisposition(filter, state, true);
        return this;
    }

    @Override
    public Receiver settle(Predicate<IncomingDelivery> filter) {
        linkState.applyDisposition(filter, null, true);
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Collection<IncomingDelivery> unsettled() {
        if (linkState.unsettledDeliveries().isEmpty()) {
            return Collections.EMPTY_LIST;
        } else {
            return Collections.unmodifiableCollection(new ArrayList<>(linkState.unsettledDeliveries().values()));
        }
    }

    //----- Internal support methods

    void remoteDisposition(Disposition disposition, ProtonIncomingDelivery delivery) {
        linkState().remoteDisposition(disposition, delivery);
    }

    @Override
    boolean isDeliveryCountInitialised() {
        return linkState.isDeliveryCountInitialised();
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
        linkState.disposition(delivery);
    }

    void deliveryRead(ProtonIncomingDelivery delivery, int bytesRead) {
        // TODO - When any resource is closed we could still allow read of inbound data but the user
        //        can't operate on the delivery so do we want that ?

        linkState.deliveryRead(delivery, bytesRead);
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
        // Nothing currently updated on this state change.
    }

    @Override
    protected void transitionedToLocallyClosed() {
        // Nothing currently updated on this state change.
    }

    @Override
    protected void transitionToRemotelyOpenedState() {
        // Nothing currently updated on this state change.
    }

    @Override
    protected void transitionToRemotelyDetachedState() {
        // Nothing currently updated on this state change.
    }

    @Override
    protected void transitionToRemotelyCosedState() {
        // Nothing currently updated on this state change.
    }

    @Override
    protected void transitionToParentLocallyClosedState() {
        // Nothing currently updated on this state change.
    }

    @Override
    protected void transitionToParentRemotelyClosedState() {
        // Nothing currently updated on this state change.
    }
}

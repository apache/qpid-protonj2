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
import java.util.function.BiConsumer;
import java.util.function.Consumer;
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
    private boolean draining;
    private boolean drained;  // TODO - This was added from old code, since we are reactive we probably don't need to retain this state.

    private final SplayMap<ProtonOutgoingDelivery> unsettled = new SplayMap<>();

    private EventHandler<OutgoingDelivery> deliveryUpdatedEventHandler = null;
    private EventHandler<Sender> sendableEventHandler = null;
    private EventHandler<LinkCreditState> drainRequestedEventHandler = null;

    private BiConsumer<ProtonOutgoingDelivery, ProtonBuffer> sendHandler = (delivery, buffer) -> {
        throw new IllegalStateException("Cannot send when sender link has not been locally opened");
    };
    private Consumer<ProtonOutgoingDelivery> dispositionHandler = delivery -> {
        throw new IllegalStateException("Cannot send a disposition when sender link has not been locally opened");
    };
    private Consumer<ProtonOutgoingDelivery> abortHandler = delivery -> {
        throw new IllegalStateException("Cannot abort a delivery when sender link has not been locally opened");
    };

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
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Sender drained(LinkCreditState state) {
        // TODO Auto-generated method stub
        return null;
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

        // TODO: null 'current' upon completion instead of checking partial here? Thats what current() doc suggests.
        if (current != null && current.isPartial()) {
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
        // int casts are expected, credit is a uint and delivery-count is really a uint sequence which wraps, so we just use the truncation and overflows.
        // Receivers flow might not have any delivery-count, as sender initialises on attach! We initialise to 0 so we can just ignore that.
        int remoteDeliveryCount = (int) flow.getDeliveryCount();
        int newDeliveryCountLimit = remoteDeliveryCount + (int) flow.getLinkCredit();

        long effectiveCredit = 0xFFFFFFFFL & newDeliveryCountLimit - existingDeliveryCount;
        if (effectiveCredit > 0) {
            creditState.updateCredit((int) effectiveCredit);
        } else {
            creditState.updateCredit(0);
        }

        draining = flow.getDrain();
        drained = getCredit() > 0;

        if (isLocallyOpen()) {
            if (getCredit() > 0 && !sendable) {
                sendable = true;
                signalSendable();
            }

            if (draining && !drained) {
                signalDrainRequested();
            }
        }

        return this;
    }

    @Override
    protected final ProtonSender decorateOutgoingFlow(Flow flow) {
        flow.setLinkCredit(getCredit());
        flow.setHandle(getHandle());
        flow.setDeliveryCount(currentDelivery.longValue());
        flow.setDrain(isDrain());

        return this;
    }

    //----- Delivery output related access points

    void send(ProtonOutgoingDelivery delivery, ProtonBuffer buffer) {
        sendHandler.accept(delivery, buffer);
    }

    void disposition(ProtonOutgoingDelivery delivery) {
        dispositionHandler.accept(delivery);
    }

    void abort(ProtonOutgoingDelivery delivery) {
        abortHandler.accept(delivery);
    }

    //----- Sender event handlers

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
        this.sendHandler = this::sendSink;

        if (sendableEventHandler != null) {
            sendableEventHandler.handle(this);
        }
        return this;
    }

    Sender signalNoLongerSendable() {
        this.sendHandler = this::senderNotWritableSink;
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

    @Override
    public boolean isDrain() {
        return false; //TODO
    }

    //----- Internal routing and state management

    @Override
    protected void transitionedToLocallyOpened() {
        localAttach.setInitialDeliveryCount(currentDelivery.longValue());

        sendHandler = (delivery, buffer) -> senderNotWritableSink(delivery, buffer);
        dispositionHandler = delivery -> dispositionSink(delivery);
        abortHandler = delivery -> abortSink(delivery);
    }

    @Override
    protected void transitionedToLocallyDetached() {
        sendable = false;

        disableAllSenderOperations("link is detached");
    }

    @Override
    protected void transitionedToLocallyClosed() {
        sendable = false;

        disableAllSenderOperations("link is closed");
    }

    @Override
    protected void transitionToRemotelyOpenedState() {
        // Nothing to do yet on remotely opened.
    }

    @Override
    protected void transitionToRemotelyDetached() {
        sendable = false;

        if (isLocallyOpen()) {
            disableAllSenderOperations("link is remotely detached");
        }
    }

    @Override
    protected void transitionToRemotelyCosed() {
        sendable = false;

        if (isLocallyOpen()) {
            disableAllSenderOperations("link is remotely closed");
        }
    }

    @Override
    protected void transitionToParentLocallyClosed() {
        if (isLocallyOpen()) {
            disableAllSenderOperations("parent resurce is locally closed");
        }
    }

    @Override
    protected void transitionToParentRemotelyClosed() {
        if (isLocallyOpen()) {
            disableAllSenderOperations("parent resurce is remotely closed");
        }
    }

    private void disableAllSenderOperations(String cause) {
        this.sendHandler = (delivery, buffer) -> {
            throw new IllegalStateException("Cannot send transfers due to: " + cause);
        };
        this.dispositionHandler = delivery -> {
            throw new IllegalStateException("Cannot send a disposition due to: " + cause);
        };
        this.abortHandler = delivery -> {
            throw new IllegalStateException("Cannot abort a delivery due to:" + cause);
        };

        // TODO - Setting this here for now, need to decide when to signal this changed.
        sendable = false;
    }

    private void sendSink(ProtonOutgoingDelivery delivery, ProtonBuffer payload) {
        if (!isSendable()) {
            // TODO - Should we check here on each write or check someplace else that
            //        the user can actually send anything.  We aren't buffering anything.
        }

        if (currentDelivery.isEmpty()) {
            currentDelivery.set(sessionWindow.getAndIncrementNextDeliveryId());

            delivery.setDeliveryId(currentDelivery.longValue());
        }

        if (!delivery.isSettled()) {
            // TODO - Casting is ugly but right now our unsigned integers are longs
            unsettled.put((int) delivery.getDeliveryId(), delivery);
        }

        sessionWindow.processSend(this, delivery, payload);

        if (!delivery.isPartial()) {
            currentDelivery.reset();
            getCreditState().incrementDeliveryCount();
            getCreditState().decrementCredit();

            if (getCredit() == 0) {
                sendable = false;
            }
        }
    }

    private void dispositionSink(ProtonOutgoingDelivery delivery) {
        if (delivery.isSettled()) {
            // TODO - Casting is ugly but right now our unsigned integers are longs
            unsettled.remove((int) delivery.getDeliveryId());
        }

        sessionWindow.processDisposition(this, delivery);
    }

    private void abortSink(ProtonOutgoingDelivery delivery) {
        // TODO - Casting is ugly but right now our unsigned integers are longs
        unsettled.remove((int) delivery.getDeliveryId());

        sessionWindow.processAbort(this, delivery);
    }

    private void senderNotWritableSink(ProtonOutgoingDelivery delivery, ProtonBuffer buffer) {
        throw new IllegalStateException("Cannot send when sender is not currently writable");
    }
}

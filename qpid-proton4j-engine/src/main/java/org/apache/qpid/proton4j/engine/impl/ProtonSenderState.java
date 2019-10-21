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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.transport.Attach;
import org.apache.qpid.proton4j.amqp.transport.DeliveryState;
import org.apache.qpid.proton4j.amqp.transport.Detach;
import org.apache.qpid.proton4j.amqp.transport.Disposition;
import org.apache.qpid.proton4j.amqp.transport.Flow;
import org.apache.qpid.proton4j.amqp.transport.Transfer;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.engine.LinkCreditState;
import org.apache.qpid.proton4j.engine.LinkState;
import org.apache.qpid.proton4j.engine.OutgoingDelivery;
import org.apache.qpid.proton4j.engine.Sender;
import org.apache.qpid.proton4j.engine.util.DeliveryIdTracker;
import org.apache.qpid.proton4j.engine.util.SplayMap;

/**
 * Credit state handler for {@link Sender} links.
*/
public class ProtonSenderState implements ProtonLinkState<ProtonOutgoingDelivery> {

    private final ProtonSender sender;
    private final ProtonSessionOutgoingWindow sessionWindow;

    private final DeliveryIdTracker currentDelivery = new DeliveryIdTracker();
    private final ProtonLinkCreditState creditState = new ProtonLinkCreditState();

    private boolean sendable;
    private boolean draining;
    private boolean drained;  // TODO - This was added from old code, since we are reactive we probably don't need to retain this state.

    private final SplayMap<ProtonOutgoingDelivery> unsettled = new SplayMap<>();

    public ProtonSenderState(ProtonSender sender, ProtonSessionOutgoingWindow sessionWindow) {
        this.sessionWindow = sessionWindow;
        this.sender = sender;
    }

    public boolean isSendable() {
        return sendable; // TODO Map other events that change sendable state into this boolean
    }

    public boolean isDraining() {
        return draining;
    }

    @Override
    public int getCredit() {
        return creditState.getCredit();
    }

    @Override
    public int getDeliveryCount() {
        return creditState.getDeliveryCount();
    }

    @Override
    public LinkCreditState snapshotCreditState() {
        return creditState.snapshot();
    }

    @Override
    public Attach configureAttach(Attach attach) {
        return attach.setInitialDeliveryCount(0);
    }

    Map<UnsignedInteger, ProtonOutgoingDelivery> unsettledDeliveries() {
        return unsettled;
    }

    //----- Handlers for processing incoming events

    @Override
    public void localClose(boolean closed) {
        creditState.setCredit(0);
        sendable = false;
        unsettled.clear();
    }

    @Override
    public void remoteAttach(Attach attach) {
        // Nothing to do yet.
    }

    @Override
    public void remoteDetach(Detach detach) {
        creditState.setCredit(0);
        sendable = false;
        unsettled.clear();
    }

    @Override
    public void remoteFlow(Flow flow) {
        creditState.setCredit((int) (flow.getDeliveryCount() + flow.getLinkCredit() - getDeliveryCount()));
        draining = flow.getDrain();
        drained = getCredit() > 0;

        if (sender.getState() == LinkState.ACTIVE) {
            // TODO - Signal for sendable, draining etc

            if (getCredit() > 0 && !sendable) {
                sendable = true;
                sender.signalSendable();
            }

            if (draining && !drained) {
                sender.signalDrainRequested();
            }
        }
    }

    @Override
    public void remoteDisposition(Disposition disposition, ProtonOutgoingDelivery delivery) {
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
    }

    @Override
    public ProtonIncomingDelivery remoteTransfer(Transfer transfer, ProtonBuffer payload) {
        throw new IllegalStateException("Cannot receive a Transfer at the Sender end of a link");
    }

    //----- Actions invoked from Delivery instances

    void send(ProtonOutgoingDelivery delivery, ProtonBuffer payload) {
        if (!isSendable()) {
            // TODO - Should we check here on each write or check someplace else that
            //        the user can actually send anything.  We aren't buffering anything.
        }

        if (currentDelivery.isEmpty()) {
            currentDelivery.set(sessionWindow.getAndIncrementNextDeliveryId());

            delivery.setDeliveryId(currentDelivery.longValue());
        }

        // TODO - If not settled we should track within the link the list of unsettled deliveries
        //        for later retrieval by a client.

        if (!delivery.isSettled()) {
            // TODO - Casting is ugly but right now our unsigned integers are longs
            unsettled.put((int) delivery.getDeliveryId(), delivery);
        }

        sessionWindow.processSend(sender, delivery, payload);

        if (!delivery.isPartial()) {
            currentDelivery.reset();
            creditState.incrementDeliveryCount();
            creditState.decrementCredit();

            if (getCredit() == 0) {
                sendable = false;
            }
        }
    }

    @SuppressWarnings("unchecked")
    void applyDisposition(Predicate<OutgoingDelivery> predicate, DeliveryState disposition, boolean settle) {
        List<UnsignedInteger> toRemove = settle ? new ArrayList<>() : Collections.EMPTY_LIST;

        unsettled.forEach((deliveryId, delivery) -> {
            if (predicate.test(delivery)) {
                if (disposition != null) {
                    delivery.localState(disposition);
                }
                if (settle) {
                    delivery.locallySettled();
                    toRemove.add(deliveryId);
                }
                sessionWindow.processDisposition(sender, delivery);
            }
        });

        if (!toRemove.isEmpty()) {
            toRemove.forEach(deliveryId -> unsettled.remove(deliveryId));
        }
    }

    void disposition(ProtonOutgoingDelivery delivery) {
        if (delivery.isSettled()) {
            // TODO - Casting is ugly but right now our unsigned integers are longs
            unsettled.remove((int) delivery.getDeliveryId());
        }

        sessionWindow.processDisposition(sender, delivery);
    }

    void abort(ProtonOutgoingDelivery delivery) {
        // TODO - Casting is ugly but right now our unsigned integers are longs
        unsettled.remove((int) delivery.getDeliveryId());

        sessionWindow.processAbort(sender, delivery);
    }
}

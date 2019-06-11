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

import java.util.Map;

import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.transport.Attach;
import org.apache.qpid.proton4j.amqp.transport.Disposition;
import org.apache.qpid.proton4j.amqp.transport.Flow;
import org.apache.qpid.proton4j.amqp.transport.Transfer;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.engine.LinkState;
import org.apache.qpid.proton4j.engine.Sender;
import org.apache.qpid.proton4j.engine.util.DeliveryIdTracker;
import org.apache.qpid.proton4j.engine.util.SplayMap;

/**
 * Credit state handler for {@link Sender} links.
 */
public class ProtonSenderCreditState implements ProtonLinkCreditState<ProtonOutgoingDelivery> {

    private final ProtonSender sender;
    private final ProtonSessionOutgoingWindow sessionWindow;

    private final DeliveryIdTracker currentDelivery = new DeliveryIdTracker();

    private boolean sendable;
    private int credit;
    private int deliveryCount;
    private boolean draining;
    private boolean drained;

    private final SplayMap<ProtonOutgoingDelivery> unsettled = new SplayMap<>();

    public ProtonSenderCreditState(ProtonSender sender, ProtonSessionOutgoingWindow sessionWindow) {
        this.sessionWindow = sessionWindow;
        this.sender = sender;
    }

    public boolean isSendable() {
        return sendable; // TODO - Session window has outbound capacity ?
    }

    public boolean isDraining() {
        return draining;
    }

    @Override
    public int getCredit() {
        return credit;
    }

    @Override
    public int getDeliveryCount() {
        return deliveryCount;
    }

    @Override
    public Attach configureAttach(Attach attach) {
        return attach.setInitialDeliveryCount(0);
    }

    @Override
    public ProtonSenderCreditState snapshot() {
        ProtonSenderCreditState snapshot = new ProtonSenderCreditState(sender, sessionWindow);
        snapshot.draining = draining;
        snapshot.credit = credit;
        snapshot.drained = drained;
        snapshot.deliveryCount = deliveryCount;
        return snapshot;
    }

    Map<UnsignedInteger, ProtonOutgoingDelivery> unsettledDeliveries() {
        return unsettled;
    }

    //----- Handlers for processing incoming events

    @Override
    public void localClose(boolean closed) {
        this.credit = 0;
        this.sendable = false;
        this.unsettled.clear();
    }

    @Override
    public void remoteAttach(Attach attach) {
        // Nothing to do yet.
    }

    @Override
    public void remoteFlow(Flow flow) {
        credit = (int) (flow.getDeliveryCount() + flow.getLinkCredit() - deliveryCount);
        draining = flow.getDrain();
        drained = credit > 0;

        if (sender.getLocalState() == LinkState.ACTIVE) {
            // TODO - Signal for sendable, draining etc

            if (credit > 0 && !sendable) {
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

            delivery.setDeliveryId(currentDelivery.byteValue());
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
            deliveryCount++;
            credit--;

            if (credit == 0) {
                sendable = false;
            }
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

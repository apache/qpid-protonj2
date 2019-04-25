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

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.transport.Attach;
import org.apache.qpid.proton4j.amqp.transport.Disposition;
import org.apache.qpid.proton4j.amqp.transport.Flow;
import org.apache.qpid.proton4j.amqp.transport.Role;
import org.apache.qpid.proton4j.amqp.transport.Transfer;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.engine.LinkState;
import org.apache.qpid.proton4j.engine.Sender;
import org.apache.qpid.proton4j.engine.util.DeliveryIdTracker;

/**
 * Credit state handler for {@link Sender} links.
 */
public class ProtonSenderCreditState implements ProtonLinkCreditState {

    private final ProtonSender sender;
    private final ProtonSessionOutgoingWindow sessionWindow;

    private final DeliveryIdTracker currentDelivery = new DeliveryIdTracker();

    private int credit;
    private int deliveryCount;
    private boolean draining;
    private boolean drained;

    public ProtonSenderCreditState(ProtonSender sender, ProtonSessionOutgoingWindow sessionWindow) {
        this.sessionWindow = sessionWindow;
        this.sender = sender;
    }

    public boolean isSendable() {
        return credit > 0; // TODO - Session window has outbound capacity ?
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

    //----- Handlers for processing incoming events

    @Override
    public Flow handleFlow(Flow flow) {
        credit = (int) (flow.getDeliveryCount() + flow.getLinkCredit() - deliveryCount);
        draining = flow.getDrain();
        drained = credit > 0;

        if (sender.getLocalState() == LinkState.ACTIVE) {
            // TODO - Signal for sendable, draining etc

            if (draining && !drained) {
                sender.signalDrainRequested();
            }
        }

        return flow;
    }

    @Override
    public Disposition handleDisposition(Disposition disposition) {
        return disposition;
    }

    @Override
    public Transfer handleTransfer(Transfer transfer, ProtonBuffer payload) {
        throw new IllegalStateException("Cannot receive a Transfer at the Sender end of a link");
    }

    //----- Actions invoked from Delivery instances

    long send(ProtonOutgoingDelivery delivery, ProtonBuffer payload) {
        if (!isSendable()) {
            // TODO - Should we check here on each write or check someplace else that
            //        the user can actually send anything.  We aren't buffering anything.
        }

        if (currentDelivery.isEmpty()) {
            currentDelivery.set(sessionWindow.getAndIncrementNextOutgoingId());
        }

        // TODO - Can we cache or pool these to not generate garbage on each send ?
        Transfer transfer = new Transfer();

        transfer.setDeliveryId(currentDelivery.longValue());
        // TODO - Delivery Tag improvements, have our own DeliveryTag type perhaps that pools etc.
        transfer.setDeliveryTag(new Binary(delivery.getTag()));
        transfer.setMore(delivery.isPartial());
        transfer.setResume(false);
        transfer.setAborted(false);
        transfer.setBatchable(false);
        transfer.setRcvSettleMode(null);
        transfer.setHandle(sender.getHandle());
        transfer.setSettled(delivery.isSettled());
        transfer.setState(delivery.getLocalState());

        // TODO - If not settled we should track within the link the list of unsettled deliveries
        //        for later retrieval by a client.

        sessionWindow.proccessTramsfer(transfer, payload);

        if (!transfer.getMore()) {
            currentDelivery.reset();
            credit--;
        }

        return transfer.getDeliveryId();
    }

    void disposition(ProtonOutgoingDelivery delivery) {
        // TODO - Can we cache or pool these to not generate garbage on each send ?
        Disposition disposition = new Disposition();

        disposition.setFirst(delivery.getDeliveryId());
        disposition.setLast(delivery.getDeliveryId());
        disposition.setRole(Role.SENDER);
        disposition.setSettled(delivery.isSettled());
        disposition.setBatchable(false);
        disposition.setState(delivery.getLocalState());

        // TODO - if settled then we can remove from the links tracked deliveries list

        sessionWindow.processDisposition(disposition);
    }

    void abort(ProtonOutgoingDelivery delivery) {
        // TODO - Can we cache or pool these to not generate garbage on each send ?
        Transfer transfer = new Transfer();

        transfer.setDeliveryId(delivery.getDeliveryId());
        transfer.setDeliveryTag(new Binary(delivery.getTag()));
        transfer.setMore(delivery.isPartial());
        transfer.setState(null);
        transfer.setSettled(false);
        transfer.setResume(false);
        transfer.setAborted(false);
        transfer.setBatchable(false);
        transfer.setRcvSettleMode(null);
        transfer.setHandle(sender.getHandle());

        // TODO - if settled then we can remove from the links tracked deliveries list

        sessionWindow.processAbort(transfer);
    }
}

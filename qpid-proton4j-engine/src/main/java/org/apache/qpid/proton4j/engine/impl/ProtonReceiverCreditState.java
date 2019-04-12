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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.proton4j.amqp.transport.Attach;
import org.apache.qpid.proton4j.amqp.transport.Disposition;
import org.apache.qpid.proton4j.amqp.transport.Flow;
import org.apache.qpid.proton4j.amqp.transport.Transfer;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.engine.exceptions.ProtocolViolationException;
import org.apache.qpid.proton4j.engine.util.DeliveryIdTracker;

/**
 * Credit state handler for {@link Receiver} links.
 */
public class ProtonReceiverCreditState implements ProtonLinkCreditState {

    private final ProtonReceiver parent;
    private final ProtonSessionIncomingWindow incomingWindow;

    private int credit;
    private int deliveryCount;

    private final DeliveryIdTracker currentDeliveryId = new DeliveryIdTracker();

    // TODO - Primitive aware storage collection
    private Map<Integer, ProtonIncomingDelivery> deliveries = new LinkedHashMap<>();

    public ProtonReceiverCreditState(ProtonReceiver parent, ProtonSessionIncomingWindow sessionWindow) {
        this.incomingWindow = sessionWindow;
        this.parent = parent;
    }

    @Override
    public int getCredit() {
        return credit;
    }

    @Override
    public int getDeliveryCount() {
        return deliveryCount;
    }

    public void setCredit(int credit) {
        if (this.credit != credit) {
            this.credit = credit;
            if (parent.isRemotelyOpened()) {
                incomingWindow.writeFlow(parent);
            }
        }
    }

    @Override
    public Attach handleAttach(Attach attach) {
        if (credit > 0) {
            incomingWindow.writeFlow(parent);
        }

        return attach;
    }

    @Override
    public Flow handleFlow(Flow flow) {
        if (flow.getDrain()) {
            deliveryCount = (int) flow.getDeliveryCount();
            credit = (int) flow.getLinkCredit();
            if (credit != 0) {
                throw new IllegalArgumentException("Receiver read flow with drain set but credit was not zero");
            }

            // TODO - Error on credit being non-zero for drain response ?

            parent.signalReceiverDrained();
        }
        return flow;
    }

    @Override
    public Transfer handleTransfer(Transfer transfer, ProtonBuffer payload) {
        final ProtonIncomingDelivery delivery;

        if (!currentDeliveryId.isEmpty() && (!transfer.hasDeliveryId() || currentDeliveryId.equals((int) transfer.getDeliveryId()))) {
            delivery = deliveries.get(currentDeliveryId.intValue());
        } else {
            verifyNewDeliveryIdSequence(transfer, currentDeliveryId);

            delivery = new ProtonIncomingDelivery(parent, transfer.getDeliveryTag());
            delivery.setMessageFormat((int) transfer.getMessageFormat());

            deliveries.put((int) transfer.getDeliveryId(), delivery);
        }

        if (transfer.hasState()) {
            delivery.setRemoteState(transfer.getState());
        }

        if (transfer.getSettled() || transfer.getAborted()) {
            delivery.remotelySettled();
        }

        delivery.appendToPayload(payload);

        boolean done = transfer.getAborted() || !transfer.getMore();
        if (done) {
            if (transfer.getAborted()) {
                delivery.aborted();
            } else {
                delivery.completed();
            }

            credit = Math.min(credit - 1, 0);
            deliveryCount++;
            currentDeliveryId.reset();
        } else {
            currentDeliveryId.set((int) transfer.getDeliveryId());
        }

        if (currentDeliveryId == null) {
            parent.signalDeliveryReceived(delivery);
        } else {
            parent.signalDeliveryUpdated(delivery);
        }

        return transfer;
    }

    private void verifyNewDeliveryIdSequence(Transfer transfer, DeliveryIdTracker currentDeliveryId) {
        // TODO - Fail engine, session, or link ?
        // TODO - Move to session window once fully worked out

        if (!transfer.hasDeliveryId()) {
            parent.getSession().getConnection().getEngine().engineFailed(
                 new ProtocolViolationException("No delivery-id specified on first Transfer of new delivery"));
        }

        // Doing a primitive comparison, uses intValue() since its a uint sequence
        // and we need the primitive values to wrap appropriately during comparison.
        if (incomingWindow.incrementNextDeliveryId() != transfer.getDeliveryId()) {
            parent.getSession().getConnection().getEngine().engineFailed(
                new ProtocolViolationException("Expected delivery-id " + incomingWindow.getNextDeliveryId() +
                                               ", got " + transfer.getDeliveryId()));
        }

        if (!currentDeliveryId.isEmpty()) {
            parent.getSession().getConnection().getEngine().engineFailed(
                new ProtocolViolationException("Illegal multiplex of deliveries on same link with delivery-id " +
                                               currentDeliveryId + " and " + transfer.getDeliveryId()));
        }
    }

    @Override
    public Disposition handleDisposition(Disposition disposition) {
        return disposition;
    }

    @Override
    public ProtonReceiverCreditState snapshot() {
        ProtonReceiverCreditState snapshot = new ProtonReceiverCreditState(parent, incomingWindow);
        snapshot.credit = credit;
        snapshot.deliveryCount = deliveryCount;
        return snapshot;
    }
}

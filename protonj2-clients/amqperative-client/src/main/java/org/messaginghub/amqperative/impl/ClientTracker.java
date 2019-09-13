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
package org.messaginghub.amqperative.impl;

import org.apache.qpid.proton4j.amqp.messaging.Accepted;
import org.apache.qpid.proton4j.engine.OutgoingDelivery;
import org.messaginghub.amqperative.DeliveryState;
import org.messaginghub.amqperative.Sender;
import org.messaginghub.amqperative.Tracker;

/**
 * Client outgoing delivery tracker object.
 */
public class ClientTracker implements Tracker {

    private final ClientSender sender;
    private final OutgoingDelivery delivery;

    /**
     * Create an instance of a client outgoing delivery tracker.
     *
     * @param sender
     *      The sender that was used to send the delivery
     * @param delivery
     *      The proton outgoing delivery object that backs this tracker.
     */
    ClientTracker(ClientSender sender, OutgoingDelivery delivery) {
        this.sender = sender;
        this.delivery = delivery;
    }

    @Override
    public Sender getSender() {
        return sender;
    }

    @Override
    public DeliveryState getLocalState() {
        return ClientDeliveryState.fromProtonType(delivery.getLocalState());
    }

    @Override
    public DeliveryState getRemoteState() {
        return ClientDeliveryState.fromProtonType(delivery.getRemoteState());
    }

    @Override
    public boolean isRemotelySettled() {
        return delivery.isRemotelySettled();
    }

    @Override
    public Tracker accept() {
        delivery.disposition(Accepted.getInstance());
        return this;
    }

    @Override
    public Tracker disposition(DeliveryState state, boolean settle) {
        org.apache.qpid.proton4j.amqp.transport.DeliveryState protonState = null;
        if (state != null) {
            try {
                protonState = ((ClientDeliveryState) state).getProtonDeliveryState();
            } catch (ClassCastException ccex) {
                throw new IllegalArgumentException("Unknown DeliveryState type given, no disposition applied to Delivery.");
            }
        }

        sender.disposition(delivery, protonState, settle);
        return this;
    }

    @Override
    public Tracker settle() {
        sender.disposition(delivery, null, true);
        return this;
    }

    @Override
    public boolean isSettled() {
        return delivery.isSettled();
    }

    @Override
    public byte[] getTag() {
        delivery.getTag();
        return null;
    }
}

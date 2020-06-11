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

import org.apache.qpid.proton4j.engine.OutgoingDelivery;
import org.messaginghub.amqperative.DeliveryState;
import org.messaginghub.amqperative.Sender;
import org.messaginghub.amqperative.Tracker;
import org.messaginghub.amqperative.futures.ClientFuture;

/**
 * Client outgoing delivery tracker object.
 */
public class ClientTracker implements Tracker {

    private final ClientSender sender;
    private final OutgoingDelivery delivery;

    private final ClientFuture<Tracker> acknowledged;

    private volatile boolean remotelySetted;
    private volatile DeliveryState remoteDeliveryState;

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
        this.acknowledged = sender.session().getFutureFactory().createFuture();
    }

    @Override
    public Sender sender() {
        return sender;
    }

    @Override
    public DeliveryState state() {
        return ClientDeliveryState.fromProtonType(delivery.getState());
    }

    @Override
    public DeliveryState remoteState() {
        return remoteDeliveryState;
    }

    @Override
    public boolean remoteSettled() {
        return remotelySetted;
    }

    @Override
    public ClientTracker disposition(DeliveryState state, boolean settle) {
        org.apache.qpid.proton4j.types.transport.DeliveryState protonState = null;
        if (state != null) {
            protonState = ClientDeliveryState.asProtonType(state);
        }

        sender.disposition(delivery, protonState, settle);
        return this;
    }

    @Override
    public ClientTracker settle() {
        sender.disposition(delivery, null, true);
        return this;
    }

    @Override
    public boolean settled() {
        return delivery.isSettled();
    }

    @Override
    public ClientFuture<Tracker> acknowledgeFuture() {
        return acknowledged;
    }

    //----- Internal Event hooks for delivery updates

    void processDeliveryUpdated(OutgoingDelivery delivery) {
        remotelySetted = delivery.isRemotelySettled();
        remoteDeliveryState = ClientDeliveryState.fromProtonType(delivery.getRemoteState());

        if (delivery.isRemotelySettled()) {
            acknowledged.complete(this);
        }
    }
}

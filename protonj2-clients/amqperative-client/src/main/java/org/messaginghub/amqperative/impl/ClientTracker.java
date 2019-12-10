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

import java.util.concurrent.Future;

import org.apache.qpid.proton4j.amqp.DeliveryTag;
import org.apache.qpid.proton4j.amqp.messaging.Accepted;
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

    private final ClientFuture<DeliveryState> remoteState;

    // TODO - Memory consistency issues are observable if auto settle is on as the
    //        settle and update of the remote and local state happens in the client
    //        thread and not on the calling thread reading the state can return the
    //        not yet updated value.

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
        this.remoteState = sender.session().getFutureFactory().createFuture();
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
    public Future<DeliveryState> remoteState() {
        return remoteState;
    }

    @Override
    public boolean remotelySettled() {
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
    public boolean settled() {
        return delivery.isSettled();
    }

    @Override
    public DeliveryTag tag() {
        return delivery.getTag();
    }

    //----- Internal Event hooks for delivery updates

    void processDeliveryUpdated(OutgoingDelivery delivery) {
        if (delivery.getRemoteState() != null || delivery.isRemotelySettled()) {
            remoteState.complete(ClientDeliveryState.fromProtonType(delivery.getRemoteState()));
        }
    }
}

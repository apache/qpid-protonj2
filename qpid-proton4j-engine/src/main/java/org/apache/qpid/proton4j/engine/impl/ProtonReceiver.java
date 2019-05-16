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

import org.apache.qpid.proton4j.amqp.transport.DeliveryState;
import org.apache.qpid.proton4j.amqp.transport.Disposition;
import org.apache.qpid.proton4j.amqp.transport.Role;
import org.apache.qpid.proton4j.engine.EventHandler;
import org.apache.qpid.proton4j.engine.IncomingDelivery;
import org.apache.qpid.proton4j.engine.Receiver;
import org.apache.qpid.proton4j.engine.Session;

/**
 * Proton Receiver link implementation.
 */
public class ProtonReceiver extends ProtonLink<Receiver> implements Receiver {

    private final ProtonReceiverCreditState creditState;

    private EventHandler<IncomingDelivery> deliveryReceivedEventHandler = null;
    private EventHandler<IncomingDelivery> deliveryUpdatedEventHandler = null;
    private EventHandler<Receiver> receiverDrainedEventHandler = null;

    private DeliveryState defaultDeliveryState;

    // TODO - On open validate that required handlers are not null

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
        this.creditState = new ProtonReceiverCreditState(this, session.getIncomingWindow());
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
    protected ProtonReceiverCreditState getCreditState() {
        return creditState;
    }

    @Override
    public int getCredit() {
        checkNotClosed("Cannot get credit on a closed Receiver");
        return creditState.getCredit();
    }

    @Override
    public ProtonReceiver setCredit(int credit) {
        checkNotClosed("Cannot set credit on a closed Receiver");
        if (credit < 0) {
            throw new IllegalArgumentException("Set credit cannot be zero");
        }

        creditState.setCredit(credit);

        return this;
    }

    //----- Internal support methods

    void handleDisposition(Disposition disposition, ProtonIncomingDelivery delivery) {
        getCreditState().handleDisposition(disposition, delivery);
    }

    //----- Delivery related access points

    void disposition(ProtonIncomingDelivery delivery) {
        // TODO - Enforce not closed etc
        creditState.disposition(delivery);
    }

    void deliveryRead(ProtonIncomingDelivery delivery, int bytesRead) {
        creditState.deliveryRead(delivery, bytesRead);
    }

    //----- Receiver event handlers

    // TODO - Don't let valid handlers be nulled unless closed

    @Override
    public Receiver deliveryReceivedEventHandler(EventHandler<IncomingDelivery> handler) {
        this.deliveryReceivedEventHandler = handler;
        return this;
    }

    Receiver signalDeliveryReceived(IncomingDelivery delivery) {
        if (deliveryReceivedEventHandler != null) {
            deliveryReceivedEventHandler.handle(delivery);
        }
        return this;
    }

    @Override
    public Receiver deliveryUpdatedEventHandler(EventHandler<IncomingDelivery> handler) {
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
    public Receiver receiverDrainedEventHandler(EventHandler<Receiver> handler) {
        this.receiverDrainedEventHandler = handler;
        return this;
    }

    Receiver signalReceiverDrained() {
        if (receiverDrainedEventHandler != null) {
            receiverDrainedEventHandler.handle(this);
        }
        return this;
    }
}

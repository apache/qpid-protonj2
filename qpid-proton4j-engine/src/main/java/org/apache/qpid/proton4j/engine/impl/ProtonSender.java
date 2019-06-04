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

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.qpid.proton4j.amqp.transport.Disposition;
import org.apache.qpid.proton4j.amqp.transport.Role;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.engine.EventHandler;
import org.apache.qpid.proton4j.engine.LinkCreditState;
import org.apache.qpid.proton4j.engine.LinkState;
import org.apache.qpid.proton4j.engine.OutgoingDelivery;
import org.apache.qpid.proton4j.engine.Sender;
import org.apache.qpid.proton4j.engine.Session;

/**
 * Proton Sender link implementation.
 */
public class ProtonSender extends ProtonLink<Sender> implements Sender {

    private final ProtonSenderCreditState creditState;

    private EventHandler<OutgoingDelivery> deliveryUpdatedEventHandler = null;
    private EventHandler<Sender> sendableEventHandler = null;
    private EventHandler<LinkCreditState> drainRequestedEventHandler = null;

    private BiConsumer<ProtonOutgoingDelivery, ProtonBuffer> sendHandler = this::senderClosedSink;
    private Consumer<ProtonOutgoingDelivery> dispositionHandler = this::senderClosedSink;
    private Consumer<ProtonOutgoingDelivery> abortHandler = this::senderClosedSink;

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
        super(session, name);
        this.creditState = new ProtonSenderCreditState(this, session.getOutgoingWindow());
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
        return creditState.getCredit();
    }

    @Override
    protected ProtonSenderCreditState getCreditState() {
        return creditState;
    }

    @Override
    public boolean isSendable() {
        return creditState.isSendable();
    }

    @Override
    public OutgoingDelivery delivery() {
        if (current == null || current.isSettled()) {
            current = new ProtonOutgoingDelivery(this);
        }

        return current;
    }

    void handleDisposition(Disposition disposition, ProtonOutgoingDelivery delivery) {
        getCreditState().handleDisposition(disposition, delivery);
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
    public Sender deliveryUpdatedEventHandler(EventHandler<OutgoingDelivery> handler) {
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
    public Sender sendableEventHandler(EventHandler<Sender> handler) {
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
    public Sender drainRequestedEventHandler(EventHandler<LinkCreditState> handler) {
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

    //----- Internal routing and state management

    @Override
    protected void transitionedToLocallyOpened() {
        // TODO - Set to not writable and handle transitions
        this.sendHandler = this::senderNotWritableSink;
        // TODO - Handle transport not writable and prevent or queue these ?
        this.dispositionHandler = this::dispositionSink;
        this.abortHandler = this::abortSink;
    }

    @Override
    protected void transitionedToLocallyDetached() {
        this.sendHandler = this::senderClosedSink;
        this.dispositionHandler = this::senderClosedSink;
        this.abortHandler = this::senderClosedSink;
    }

    @Override
    protected void transitionedToLocallyClosed() {
        this.sendHandler = this::senderClosedSink;
        this.dispositionHandler = this::senderClosedSink;
        this.abortHandler = this::senderClosedSink;
    }

    private void sendSink(ProtonOutgoingDelivery delivery, ProtonBuffer payload) {
        creditState.send(delivery, payload);
    }

    private void dispositionSink(ProtonOutgoingDelivery delivery) {
        creditState.disposition(delivery);
    }

    private void abortSink(ProtonOutgoingDelivery delivery) {
        creditState.abort(delivery);
    }

    private void senderClosedSink(ProtonOutgoingDelivery delivery, ProtonBuffer buffer) {
        if (getLocalState() == LinkState.DETACHED) {
            throw new IllegalStateException("Cannot send a disposition when sender link is detached");
        } else {
            throw new IllegalStateException("Cannot send a disposition when sender link is closed");
        }
    }

    private void senderClosedSink(ProtonOutgoingDelivery delivery) {
        throw new IllegalStateException("Cannot send a disposition when sender link is closed");
    }

    private void senderNotWritableSink(ProtonOutgoingDelivery delivery, ProtonBuffer buffer) {
        throw new IllegalStateException("Cannot send disposition when sender is not currently writable");
    }

    private void senderNotWritableSink(ProtonOutgoingDelivery delivery) {
        throw new IllegalStateException("Cannot send disposition when sender is not currently writable");
    }
}

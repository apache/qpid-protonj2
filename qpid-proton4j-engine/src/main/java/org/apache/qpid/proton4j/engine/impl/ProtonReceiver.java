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

import org.apache.qpid.proton4j.amqp.transport.Disposition;
import org.apache.qpid.proton4j.amqp.transport.Role;
import org.apache.qpid.proton4j.amqp.transport.Transfer;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.engine.EventHandler;
import org.apache.qpid.proton4j.engine.IncomingDelivery;
import org.apache.qpid.proton4j.engine.Receiver;
import org.apache.qpid.proton4j.engine.Session;

/**
 * Proton Receiver link implementation.
 */
public class ProtonReceiver extends ProtonLink<Receiver> implements Receiver {

    private final ProtonReceiverCreditState creditState;

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
    public ProtonReceiver setCredit(int credit) {
        checkNotClosed("Cannot set credit on a closed Receiver");
        if (credit < 0) {
            throw new IllegalArgumentException("Set credit cannot be zero");
        }

        creditState.setCredit(credit);

        return this;
    }

    //----- Handle incoming performatives

    @Override
    public void handleTransfer(Transfer transfer, ProtonBuffer payload, int channel, ProtonEngine context) {
        creditState.handleTransfer(transfer, payload);
    }

    @Override
    public void handleDisposition(Disposition disposition, ProtonBuffer payload, int channel, ProtonEngine context) {

    }

    //----- Receiver event handlers

    @Override
    public Receiver deliveryReceivedEventHandler(EventHandler<IncomingDelivery> handler) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public Receiver deliveryUpdatedEventHandler(EventHandler<IncomingDelivery> handler) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public Receiver receiverDrainedEventHandler(EventHandler<Receiver> handler) {
        // TODO Auto-generated method stub
        return this;
    }
}

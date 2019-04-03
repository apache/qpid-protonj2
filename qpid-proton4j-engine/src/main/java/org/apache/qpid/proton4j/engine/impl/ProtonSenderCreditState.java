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

import org.apache.qpid.proton4j.amqp.transport.Attach;
import org.apache.qpid.proton4j.amqp.transport.Flow;
import org.apache.qpid.proton4j.amqp.transport.Transfer;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.engine.LinkState;
import org.apache.qpid.proton4j.engine.Sender;

/**
 * Credit state handler for {@link Sender} links.
 */
public class ProtonSenderCreditState implements ProtonLinkCreditState {

    private final ProtonSender parent;
    private final ProtonSessionWindow sessionWindow;

    private int credit;
    private int deliveryCount;
    private boolean draining;

    public ProtonSenderCreditState(ProtonSender parent, ProtonSessionWindow sessionWindow) {
        this.sessionWindow = sessionWindow;
        this.parent = parent;
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
    public Attach configureOutbound(Attach attach) {
        return attach.setInitialDeliveryCount(0);
    }

    @Override
    public Flow handleFlow(Flow flow) {
        credit = (int) (flow.getDeliveryCount() + flow.getLinkCredit() - deliveryCount);
        draining = flow.getDrain();

        if (parent.getLocalState() == LinkState.ACTIVE) {
            // TODO - Signal for sendable, draining etc
        }

        return flow;
    }

    @Override
    public Transfer handleTransfer(Transfer transfer, ProtonBuffer payload) {
        throw new IllegalStateException("Cannot receive a Transfer at the Sender end.");
    }

    @Override
    public ProtonSenderCreditState snapshot() {
        ProtonSenderCreditState snapshot = new ProtonSenderCreditState(parent, sessionWindow);
        snapshot.draining = draining;
        snapshot.credit = credit;
        snapshot.deliveryCount = deliveryCount;
        return snapshot;
    }
}

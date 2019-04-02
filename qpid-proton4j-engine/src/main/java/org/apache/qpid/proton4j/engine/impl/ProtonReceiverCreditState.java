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

import org.apache.qpid.proton4j.amqp.transport.Flow;
import org.apache.qpid.proton4j.amqp.transport.Transfer;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;

/**
 * Credit state handler for {@link Receiver} links.
 */
public class ProtonReceiverCreditState extends ProtonLinkCreditState {

    private final ProtonReceiver parent;

    public ProtonReceiverCreditState(ProtonReceiver parent, ProtonSessionWindow sessionWindow) {
        super(sessionWindow);

        this.parent = parent;
    }

    public void setCredit(int credit) {
        if (this.credit != credit) {
            this.credit = credit;
            // TODO write new flow with updated credit
        }
    }

    @Override
    Flow handleFlow(Flow flow) {
        if (flow.getDrain()) {
            deliveryCount = (int) flow.getDeliveryCount();
            credit = (int) flow.getLinkCredit();
            if (credit != 0) {
                throw new IllegalArgumentException("Receiver read flow with drain set but credit was not zero");
            }

            // TODO - Echo if requested

            // TODO - Fire event to registered listener that link was drained.
        }
        return flow;
    }

    @Override
    Transfer handleTransfer(Transfer transfer, ProtonBuffer payload) {
        return null;
    }

    @Override
    ProtonReceiverCreditState snapshot() {
        ProtonReceiverCreditState snapshot = new ProtonReceiverCreditState(parent, sessionWindow);
        copyInto(snapshot);
        return snapshot;
    }
}

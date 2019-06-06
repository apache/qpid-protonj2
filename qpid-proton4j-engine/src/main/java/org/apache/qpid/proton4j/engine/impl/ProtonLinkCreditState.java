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
import org.apache.qpid.proton4j.amqp.transport.Disposition;
import org.apache.qpid.proton4j.amqp.transport.Flow;
import org.apache.qpid.proton4j.amqp.transport.Transfer;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.engine.Delivery;
import org.apache.qpid.proton4j.engine.LinkCreditState;

/**
 * Proton LinkCreditState base used to define common API amongst the implementations.
 *
 * @param <DeliveryType>
 */
public interface ProtonLinkCreditState<DeliveryType extends Delivery> extends LinkCreditState {

    /**
     * Creates a snapshot of the current credit state, a subclass should implement this
     * method and provide an appropriately populated snapshot of the current state.
     *
     * @return a snapshot of the current credit state.
     */
    ProtonLinkCreditState<?> snapshot();

    /**
     * Initialize link state on an outbound Attach for this link
     *
     * @param attach
     *      the {@link Attach} performative that will be sent.
     *
     * @return the attach object for chaining
     */
    default Attach configureAttach(Attach attach) {
        return attach;
    }

    /**
     * Perform any needed cleanup or state change when the parent link instance is locally
     * closed or detached.
     *
     * @param closed
     *      indicates if the link was closed or detached
     */
    abstract void localClose(boolean closed);

    /**
     * Perform any needed initialization for link credit based on the initial Attach
     * sent from the remote
     *
     * @param attach
     */
    abstract void remoteAttach(Attach attach);

    /**
     * Handle incoming {@link Flow} performatives and update link credit accordingly.
     *
     * @param flow
     *      The {@link Flow} instance to be processed.
     */
    abstract void remoteFlow(Flow flow);

    /**
     * Handle incoming {@link Transfer} performatives and update link credit accordingly.
     *
     * @param transfer
     *      The {@link Transfer} instance to be processed.
     * @param payload
     *      The buffer containing the payload of the incoming {@link Transfer}
     *
     * @return the incoming delivery associated with this transfer
     */
    abstract ProtonIncomingDelivery remoteTransfer(Transfer transfer, ProtonBuffer payload);

    /**
     * Handle incoming {@link Disposition} performatives and update link accordingly.
     *
     * @param disposition
     *      The {@link Disposition} instance to be processed.
     * @param delivery
     *      The {@link Delivery} that is the target of this disposition.
     */
    abstract void remoteDisposition(Disposition disposition, DeliveryType delivery);

}

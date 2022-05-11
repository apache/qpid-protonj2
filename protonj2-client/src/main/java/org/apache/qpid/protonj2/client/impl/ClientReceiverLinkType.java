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

package org.apache.qpid.protonj2.client.impl;

import org.apache.qpid.protonj2.client.Link;
import org.apache.qpid.protonj2.client.LinkOptions;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.engine.IncomingDelivery;
import org.apache.qpid.protonj2.engine.Receiver;
import org.apache.qpid.protonj2.types.transport.DeliveryState;

/**
 * Base class for client link types that wrap a proton receiver to provide
 * delivery dispatch in some manner.
 */
public abstract class ClientReceiverLinkType<ReceiverType extends Link<ReceiverType>> extends ClientLinkType<ReceiverType, Receiver> {

    protected Receiver protonReceiver;

    protected ClientReceiverLinkType(ClientSession session, String linkId, LinkOptions<?> options, Receiver protonReceiver) {
        super(session, linkId, options);

        this.protonReceiver = protonReceiver;
    }

    @Override
    protected org.apache.qpid.protonj2.engine.Receiver protonLink() {
        return protonReceiver;
    }

    /**
     * Apply the given disposition and settlement state to the given incoming delivery instance.
     *
     * @param delivery
     * 		The incoming delivery that will be acted upon
     * @param state
     * 		The delivery state to apply to the given incoming delivery
     * @param settle
     * 		The settlement state to apply to the given incoming delivery
     *
     * @throws ClientException if an error occurs while applying the disposition to the delivery.
     */
    abstract void disposition(IncomingDelivery delivery, DeliveryState state, boolean settle) throws ClientException;

}

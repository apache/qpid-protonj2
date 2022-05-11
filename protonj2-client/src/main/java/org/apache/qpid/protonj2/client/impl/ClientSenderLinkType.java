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
import org.apache.qpid.protonj2.client.exceptions.ClientUnsupportedOperationException;
import org.apache.qpid.protonj2.engine.LinkState;
import org.apache.qpid.protonj2.engine.OutgoingDelivery;
import org.apache.qpid.protonj2.engine.Sender;
import org.apache.qpid.protonj2.types.transport.DeliveryState;
import org.apache.qpid.protonj2.types.transport.SenderSettleMode;

/**
 * Base type for all the proton client sender types which provides a few extra
 * APIs for the connection and session to use when managing senders
 */
public abstract class ClientSenderLinkType<LinkType extends Link<LinkType>> extends ClientLinkType<LinkType, Sender> {

    private final boolean sendsSettled;

    protected Sender protonSender;

    protected ClientSenderLinkType(ClientSession session, String linkId, LinkOptions<?> options, Sender protonSender) {
        super(session, linkId, options);

        this.protonSender = protonSender;
        this.protonSender = protonSender.setLinkedResource(self());
        this.sendsSettled = protonSender.getSenderSettleMode() == SenderSettleMode.SETTLED;
    }

    final boolean isAnonymous() {
        return protonSender.<org.apache.qpid.protonj2.types.messaging.Target>getTarget().getAddress() == null;
    }

    final boolean isSendingSettled() {
        return sendsSettled;
    }

    @Override
    final protected Sender protonLink() {
        return protonSender;
    }

    final void handleAnonymousRelayNotSupported() {
        if (isAnonymous() && protonSender.getState() == LinkState.IDLE) {
            immediateLinkShutdown(new ClientUnsupportedOperationException("Anonymous relay support not available from this connection"));
        }
    }

    /**
     * Provides a common API point for sender link types to allow resources to process and then
     * apply dispositions to outgoing delivery types.
     *
     * @param delivery
     * 		The outgoing delivery to apply the disposition to
     * @param state
     * 		The delivery state to apply to the outgoing delivery.
     * @param settled
     * 		Should the outgoing delivery be settled as part of the disposition.
     *
     * @throws ClientException if an error occurs while applying the dispostion.
     */
    abstract void disposition(OutgoingDelivery delivery, DeliveryState state, boolean settled) throws ClientException;

}

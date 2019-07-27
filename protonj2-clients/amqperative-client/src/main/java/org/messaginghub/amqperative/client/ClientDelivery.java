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
package org.messaginghub.amqperative.client;

import org.apache.qpid.proton4j.amqp.messaging.Accepted;
import org.apache.qpid.proton4j.amqp.messaging.Modified;
import org.apache.qpid.proton4j.amqp.messaging.Rejected;
import org.apache.qpid.proton4j.amqp.messaging.Released;
import org.apache.qpid.proton4j.amqp.transactions.TransactionalState;
import org.apache.qpid.proton4j.engine.IncomingDelivery;
import org.messaginghub.amqperative.Delivery;
import org.messaginghub.amqperative.DeliveryState;
import org.messaginghub.amqperative.Message;
import org.messaginghub.amqperative.Receiver;

/**
 * Client inbound delivery object.
 */
public class ClientDelivery implements Delivery {

    private final ClientReceiver receiver;
    private final IncomingDelivery delivery;

    /**
     * Creates a new client delivery object linked to the given {@link IncomingDelivery}
     * instance.
     *
     * @param receiver
     *      The {@link Receiver} that processed this delivery.
     * @param delivery
     *      The proton incoming delivery that backs this client delivery facade.
     */
    ClientDelivery(ClientReceiver receiver, IncomingDelivery delivery) {
        this.receiver = receiver;
        this.delivery = delivery;
    }

    @Override
    public Message getMessage() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Delivery accept() {
        delivery.disposition(Accepted.getInstance());
        return this;
    }

    @Override
    public Delivery disposition(DeliveryState state, boolean settle) {
        org.apache.qpid.proton4j.amqp.transport.DeliveryState protonState = null;
        if (state != null) {
            // TODO - Create simpler DeliveryState object for client side ?
            switch (state.getType()) {
                case ACCEPTED:
                    protonState = Accepted.getInstance();
                case MODIFIED:
                    protonState = new Modified();
                case REJECTED:
                    protonState = new Rejected();
                case RELEASED:
                    protonState = Released.getInstance();
                case TRANSACTIONAL:
                    protonState = new TransactionalState();
                default:
                    throw new IllegalArgumentException("Unknown DeliveryState type given");
            }
        }

        receiver.disposition(delivery, protonState, settle);
        return this;
    }

    @Override
    public Delivery settle() {
        receiver.disposition(delivery, null, true);
        return this;
    }

    @Override
    public DeliveryState getLocalState() {
        // TODO - Create simple mapping builder in our DeliveryState implementation
        return null;
    }

    @Override
    public DeliveryState getRemoteState() {
        // TODO - Create simple mapping builder in our DeliveryState implementation
        return null;
    }

    @Override
    public boolean isRemotelySettled() {
        return delivery.isRemotelySettled();
    }

    @Override
    public byte[] getTag() {
        return delivery.getTag();
    }

    @Override
    public int getMessageFormat() {
        return delivery.getMessageFormat();
    }
}

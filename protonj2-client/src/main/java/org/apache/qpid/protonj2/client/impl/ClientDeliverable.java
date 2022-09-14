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

import org.apache.qpid.protonj2.client.DeliveryState;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.engine.IncomingDelivery;
import org.apache.qpid.protonj2.types.messaging.Accepted;
import org.apache.qpid.protonj2.types.messaging.Modified;
import org.apache.qpid.protonj2.types.messaging.Rejected;
import org.apache.qpid.protonj2.types.messaging.Released;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;

/**
 * Abstract type that implements some of the common portions of a delivery
 * wrapper type.
 *
 * @param <DeliveryType> The client delivery type streamed or non-streamed
 * @param <ReceiverType> The client receiver type streaming or non-streaming
 */
@SuppressWarnings("rawtypes")
public abstract class ClientDeliverable<DeliveryType, ReceiverType extends ClientReceiverLinkType> {

    protected final ReceiverType receiver;
    protected final IncomingDelivery delivery;

    ClientDeliverable(ReceiverType receiver, IncomingDelivery delivery) {
        this.receiver = receiver;
        this.delivery = delivery;
        this.delivery.setLinkedResource(self());
    }

    protected abstract DeliveryType self();

    IncomingDelivery protonDelivery() {
        return delivery;
    }

    public DeliveryType accept() throws ClientException {
        receiver.disposition(delivery, Accepted.getInstance(), true);
        return self();
    }

    public DeliveryType release() throws ClientException {
        receiver.disposition(delivery, Released.getInstance(), true);
        return self();
    }

    public DeliveryType reject(String condition, String description) throws ClientException {
        receiver.disposition(delivery, new Rejected().setError(new ErrorCondition(condition, description)), true);
        return self();
    }

    public DeliveryType modified(boolean deliveryFailed, boolean undeliverableHere) throws ClientException {
        receiver.disposition(delivery, new Modified().setDeliveryFailed(deliveryFailed).setUndeliverableHere(undeliverableHere), true);
        return self();
    }

    public DeliveryType disposition(DeliveryState state, boolean settle) throws ClientException {
        receiver.disposition(delivery, ClientDeliveryState.asProtonType(state), settle);
        return self();
    }

    public DeliveryType settle() throws ClientException {
        receiver.disposition(delivery, null, true);
        return self();
    }

    public DeliveryState state() {
        return ClientDeliveryState.fromProtonType(delivery.getState());
    }

    public DeliveryState remoteState() {
        return ClientDeliveryState.fromProtonType(delivery.getRemoteState());
    }

    public boolean remoteSettled() {
        return delivery.isRemotelySettled();
    }

    public int messageFormat() {
        return delivery.getMessageFormat();
    }

    public boolean settled() {
        return delivery.isSettled();
    }
}

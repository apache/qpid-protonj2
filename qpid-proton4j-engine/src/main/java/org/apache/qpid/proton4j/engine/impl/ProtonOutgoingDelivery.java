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

import org.apache.qpid.proton4j.amqp.DeliveryTag;
import org.apache.qpid.proton4j.amqp.transport.DeliveryState;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.engine.OutgoingDelivery;

/**
 * Proton outgoing delivery implementation
 */
public class ProtonOutgoingDelivery implements OutgoingDelivery {

    private static final long DELIVERY_INACTIVE = -1;
    private static final long DELIVERY_ABORTED = -2;

    private final ProtonContext context = new ProtonContext();
    private final ProtonSender link;

    private long deliveryId = DELIVERY_INACTIVE;

    // TODO - Creating an internal system for generating pooled tags so
    //        that the user doesn't need to manage them would be nice
    private DeliveryTag deliveryTag;

    private boolean complete;
    private int messageFormat;
    private boolean aborted;

    private DeliveryState localState;
    private boolean locallySettled;

    private DeliveryState remoteState;
    private boolean remotelySettled;

    private final ProtonBuffer payload = ProtonByteBufferAllocator.DEFAULT.allocate();

    public ProtonOutgoingDelivery(ProtonSender link) {
        this.link = link;
    }

    @Override
    public ProtonSender getLink() {
        return link;
    }

    @Override
    public ProtonContext getContext() {
        return context;
    }

    @Override
    public DeliveryTag getTag() {
        return deliveryTag;
    }

    @Override
    public OutgoingDelivery setTag(byte[] deliveryTag) {
        this.deliveryTag = new DeliveryTag.ProtonDeliveryTag(deliveryTag);
        return this;
    }

    @Override
    public OutgoingDelivery setTag(DeliveryTag deliveryTag) {
        this.deliveryTag = deliveryTag;
        return this;
    }

    @Override
    public DeliveryState getState() {
        return localState;
    }

    @Override
    public DeliveryState getRemoteState() {
        return remoteState;
    }

    @Override
    public int getMessageFormat() {
        return messageFormat;
    }

    @Override
    public OutgoingDelivery setMessageFormat(int messageFormat) {
        this.messageFormat = messageFormat;
        return this;
    }

    @Override
    public boolean isPartial() {
        return !complete && !aborted;
    }

    @Override
    public boolean isAborted() {
        return aborted;
    }

    @Override
    public boolean isSettled() {
        return locallySettled;
    }

    @Override
    public boolean isRemotelySettled() {
        return remotelySettled;
    }

    @Override
    public OutgoingDelivery disposition(DeliveryState state) {
        return disposition(state, false);
    }

    @Override
    public OutgoingDelivery disposition(DeliveryState state, boolean settle) {
        if (locallySettled) {
            if ((localState != null && !localState.equals(state)) || localState != state) {
                throw new IllegalStateException("Cannot update disposition on an already settled Delivery");
            } else {
                return this;
            }
        }

        this.locallySettled = settle;
        this.localState = state;

        // If no transfers initiated yet we just store the state and transmit in the first transfer
        if (deliveryId > DELIVERY_INACTIVE) {
            link.disposition(this);
        }

        return this;
    }

    @Override
    public OutgoingDelivery settle() {
        return disposition(localState, true);
    }

    @Override
    public OutgoingDelivery writeBytes(ProtonBuffer buffer) {
        checkCompleteOrAborted();
        payload.writeBytes(buffer);  // TODO don't copy if we can
        complete = true;
        link.send(this, payload);
        return this;
    }

    @Override
    public OutgoingDelivery streamBytes(ProtonBuffer buffer) {
        return streamBytes(buffer, false);
    }

    @Override
    public OutgoingDelivery streamBytes(ProtonBuffer buffer, boolean complete) {
        checkCompleteOrAborted();
        payload.writeBytes(buffer);  // TODO don't copy if we can
        this.complete = complete;
        link.send(this, payload);
        return this;
    }

    @Override
    public OutgoingDelivery abort() {
        checkComplete();

        // Cannot abort when nothing has been sent so far.
        if (deliveryId > DELIVERY_INACTIVE) {
            aborted = true;
            locallySettled = true;
            link.abort(this);
            deliveryId = DELIVERY_ABORTED;
        }

        return this;
    }

    //----- Internal methods meant only for use by Proton resources

    long getDeliveryId() {
        return deliveryId;
    }

    void setDeliveryId(long deliveryId) {
        this.deliveryId = deliveryId;
    }

    void afterTransferWritten() {
        // TODO - Perform any cleanup needed like reclaiming buffer space if there
        //        is a composite or other complex buffer type in use.
    }

    ProtonOutgoingDelivery remotelySettled() {
        this.remotelySettled = true;
        return this;
    }

    ProtonOutgoingDelivery remoteState(DeliveryState remoteState) {
        this.remoteState = remoteState;
        return this;
    }

    ProtonOutgoingDelivery locallySettled() {
        this.locallySettled = true;
        return this;
    }

    ProtonOutgoingDelivery localState(DeliveryState localState) {
        this.localState = localState;
        return this;
    }

    //----- Private helper methods

    private void checkComplete() {
        if (complete) {
            throw new IllegalArgumentException("Cannot write to a delivery already marked as complete.");
        }
    }

    private void checkCompleteOrAborted() {
        if (complete || aborted) {
            throw new IllegalArgumentException("Cannot write to a delivery already marked as complete or has been aborted.");
        }
    }
}

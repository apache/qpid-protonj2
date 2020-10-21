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
package org.apache.qpid.protonj2.engine.impl;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.engine.OutgoingDelivery;
import org.apache.qpid.protonj2.types.DeliveryTag;
import org.apache.qpid.protonj2.types.transport.DeliveryState;

/**
 * Proton outgoing delivery implementation
 */
public class ProtonOutgoingDelivery implements OutgoingDelivery {

    private static final long DELIVERY_INACTIVE = -1;
    private static final long DELIVERY_ABORTED = -2;

    private final ProtonSender link;

    private long deliveryId = DELIVERY_INACTIVE;

    private DeliveryTag deliveryTag;

    private boolean complete;
    private int messageFormat;
    private boolean aborted;
    private int transferCount;

    private DeliveryState localState;
    private boolean locallySettled;

    private DeliveryState remoteState;
    private boolean remotelySettled;

    private ProtonAttachments attachments;
    private Object linkedResource;

    public ProtonOutgoingDelivery(ProtonSender link) {
        this.link = link;
    }

    @Override
    public ProtonSender getLink() {
        return link;
    }

    @Override
    public ProtonAttachments getAttachments() {
        return attachments == null ? attachments = new ProtonAttachments() : attachments;
    }

    @Override
    public ProtonOutgoingDelivery setLinkedResource(Object resource) {
        this.linkedResource = resource;
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getLinkedResource() {
        return (T) linkedResource;
    }

    @Override
    public <T> T getLinkedResource(Class<T> typeClass) {
        return typeClass.cast(linkedResource);
    }

    @Override
    public DeliveryTag getTag() {
        return deliveryTag;
    }

    @Override
    public OutgoingDelivery setTag(byte[] deliveryTag) {
        if (transferCount > 0) {
            throw new IllegalStateException("Cannot change delivery tag once Delivery has sent Transfer frames");
        }

        if (this.deliveryTag != null) {
            this.deliveryTag.release();
            this.deliveryTag = null;
        }

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
        if (transferCount > 0 && this.messageFormat != messageFormat) {
            throw new IllegalStateException("Cannot change the message format once Delivery has sent Transfer frames");
        }

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

        final DeliveryState oldState = localState;

        this.locallySettled = settle;
        this.localState = state;

        // If no transfers initiated yet we just store the state and transmit in the first transfer
        // and if no work actually requested we don't emit a useless frame.  After complete send we
        // must send a disposition instead for this transfer until it is settled.
        if (complete && (oldState != localState || settle)) {
            try {
                link.disposition(this);
            } finally {
                tryRetireDeliveryId();
            }
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
        try {
            link.send(this, buffer, true);
        } finally {
            tryRetireDeliveryId();
        }
        return this;
    }

    @Override
    public OutgoingDelivery streamBytes(ProtonBuffer buffer) {
        return streamBytes(buffer, false);
    }

    @Override
    public OutgoingDelivery streamBytes(ProtonBuffer buffer, boolean complete) {
        checkCompleteOrAborted();
        try {
            link.send(this, buffer, complete);
        } finally {
            tryRetireDeliveryId();
        }
        return this;
    }

    @Override
    public OutgoingDelivery abort() {
        checkComplete();

        // Cannot abort when nothing has been sent so far.
        if (deliveryId > DELIVERY_INACTIVE) {
            aborted = true;
            locallySettled = true;
            try {
                link.abort(this);
            } finally {
                tryRetireDeliveryId();
                deliveryId = DELIVERY_ABORTED;
            }
        }

        return this;
    }

    //----- Internal methods meant only for use by Proton resources

    private void tryRetireDeliveryId() {
        if (deliveryTag != null && isSettled()) {
            deliveryTag.release();
        }
    }

    long getDeliveryId() {
        return deliveryId;
    }

    void setDeliveryId(long deliveryId) {
        this.deliveryId = deliveryId;
    }

    int getTransferCount() {
        return transferCount;
    }

    void afterTransferWritten() {
        transferCount++;
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

    ProtonOutgoingDelivery markComplete() {
        this.complete = true;
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

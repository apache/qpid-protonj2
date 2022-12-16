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
import org.apache.qpid.protonj2.buffer.ProtonCompositeBuffer;
import org.apache.qpid.protonj2.engine.EventHandler;
import org.apache.qpid.protonj2.engine.IncomingDelivery;
import org.apache.qpid.protonj2.types.DeliveryTag;
import org.apache.qpid.protonj2.types.transport.DeliveryState;

/**
 * Proton Incoming Delivery implementation
 */
public class ProtonIncomingDelivery implements IncomingDelivery {

    private final DeliveryTag deliveryTag;
    private final ProtonReceiver link;
    private final long deliveryId;

    private boolean complete;
    private int messageFormat;
    private boolean aborted;
    private int transferCount;
    private int claimedBytes;

    private DeliveryState defaultDeliveryState;

    private DeliveryState localState;
    private boolean locallySettled;

    private DeliveryState remoteState;
    private boolean remotelySettled;

    private ProtonBuffer payload;

    private ProtonAttachments attachments;
    private Object linkedResource;

    private EventHandler<IncomingDelivery> deliveryReadEventHandler = null;
    private EventHandler<IncomingDelivery> deliveryAbortedEventHandler = null;
    private EventHandler<IncomingDelivery> deliveryUpdatedEventHandler = null;

    /**
     * @param link        The link that this delivery is associated with
     * @param deliveryId  The Delivery Id that is assigned to this delivery.
     * @param deliveryTag The delivery tag assigned to this delivery
     */
    public ProtonIncomingDelivery(ProtonReceiver link, long deliveryId, DeliveryTag deliveryTag) {
        this.deliveryId = deliveryId;
        this.deliveryTag = deliveryTag;
        this.link = link;
    }

    @Override
    public ProtonReceiver getLink() {
        return link;
    }

    @Override
    public ProtonAttachments getAttachments() {
        return attachments == null ? attachments = new ProtonAttachments() : attachments;
    }

    @Override
    public ProtonIncomingDelivery setLinkedResource(Object resource) {
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

    ProtonIncomingDelivery setMessageFormat(int messageFormat) {
        this.messageFormat = messageFormat;
        return this;
    }

    @Override
    public boolean isPartial() {
        return !complete || aborted;
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
    public ProtonIncomingDelivery setDefaultDeliveryState(DeliveryState state) {
        this.defaultDeliveryState = state;
        return this;
    }

    @Override
    public DeliveryState getDefaultDeliveryState() {
        return defaultDeliveryState;
    }

    @Override
    public IncomingDelivery disposition(DeliveryState state) {
        return disposition(state, false);
    }

    @Override
    public IncomingDelivery disposition(DeliveryState state, boolean settle) {
        if (locallySettled) {
            if ((localState != null && !localState.equals(state)) || localState != state) {
                throw new IllegalStateException("Cannot update disposition on an already settled Delivery");
            } else {
                return this;
            }
        }

        this.locallySettled = settle;
        this.localState = state;
        this.link.disposition(this);

        return this;
    }

    @Override
    public IncomingDelivery settle() {
        return disposition(localState, true);
    }

    // ----- Payload access

    @Override
    public int available() {
        return payload == null ? 0 : payload.getReadableBytes();
    }

    @Override
    public ProtonBuffer readAll() {
        ProtonBuffer result = null;
        if (payload != null) {
            final int bytesRead = claimedBytes -= payload.getReadableBytes();
            result = payload.transfer();
            payload = null;

            if (bytesRead < 0) {
                claimedBytes = 0;
                link.deliveryRead(this, -bytesRead);
            }
        }

        return result;
    }

    @Override
    public ProtonIncomingDelivery readBytes(ProtonBuffer buffer) {
        if (payload != null) {
            int bytesRead = Math.min(payload.getReadableBytes(), buffer.getWritableBytes());
            payload.copyInto(payload.getReadOffset(), buffer, buffer.getWriteOffset(), bytesRead);
            payload.advanceReadOffset(bytesRead);

            if (complete && !payload.isReadable()) {
                payload = null;
            }

            bytesRead = claimedBytes -= bytesRead;
            if (claimedBytes < 0) {
                claimedBytes = 0;
                link.deliveryRead(this, -bytesRead);
            }
        }
        return this;
    }

    @Override
    public ProtonIncomingDelivery readBytes(byte[] array, int offset, int length) {
        if (payload != null) {
            int bytesRead = payload.getReadableBytes();
            payload.readBytes(array, offset, length);
            bytesRead -= payload.getReadableBytes();
            if (complete && !payload.isReadable()) {
                payload = null;
            }

            bytesRead = claimedBytes -= bytesRead;
            if (bytesRead < 0) {
                claimedBytes = 0;
                link.deliveryRead(this, -bytesRead);
            }
        }
        return this;
    }

    @Override
    public IncomingDelivery claimAvailableBytes() {
        long unclaimed = available();

        if (unclaimed > 0) {
            unclaimed -= claimedBytes;
            if (unclaimed > 0) {
                claimedBytes += unclaimed;
                link.deliveryRead(this, (int) unclaimed);
            }
        }

        return this;
    }

    // ----- Incoming Delivery event handlers

    @Override
    public ProtonIncomingDelivery deliveryReadHandler(EventHandler<IncomingDelivery> handler) {
        this.deliveryReadEventHandler = handler;
        return this;
    }

    EventHandler<IncomingDelivery> deliveryReadHandler() {
        return deliveryReadEventHandler;
    }

    @Override
    public ProtonIncomingDelivery deliveryAbortedHandler(EventHandler<IncomingDelivery> handler) {
        this.deliveryAbortedEventHandler = handler;
        return this;
    }

    EventHandler<IncomingDelivery> deliveryAbortedHandler() {
        return deliveryAbortedEventHandler;
    }

    @Override
    public ProtonIncomingDelivery deliveryStateUpdatedHandler(EventHandler<IncomingDelivery> handler) {
        this.deliveryUpdatedEventHandler = handler;
        return this;
    }

    EventHandler<IncomingDelivery> deliveryStateUpdatedHandler() {
        return deliveryUpdatedEventHandler;
    }

    // ----- Internal methods to manage the Delivery

    @Override
    public int getTransferCount() {
        return transferCount;
    }

    boolean isFirstTransfer() {
        return transferCount <= 1;
    }

    long getDeliveryId() {
        return deliveryId;
    }

    int getDeliveryIdInt() {
        return (int) deliveryId;
    }

    ProtonIncomingDelivery aborted() {
        aborted = true;

        if (payload != null) {
            final int bytesRead = payload.getReadableBytes();

            payload.close();
            payload = null;

            // Ensure Session no longer records these in the window metrics
            link.deliveryRead(this, bytesRead);
        }

        return this;
    }

    ProtonIncomingDelivery completed() {
        this.complete = true;
        return this;
    }

    ProtonIncomingDelivery remotelySettled() {
        this.remotelySettled = true;
        return this;
    }

    ProtonIncomingDelivery remoteState(DeliveryState remoteState) {
        this.remoteState = remoteState;
        return this;
    }

    ProtonIncomingDelivery locallySettled() {
        this.locallySettled = true;
        return this;
    }

    ProtonIncomingDelivery localState(DeliveryState localState) {
        this.localState = localState;
        return this;
    }

    ProtonIncomingDelivery appendTransferPayload(ProtonBuffer buffer) {
        transferCount++;

        if (payload == null) {
            this.payload = buffer;
        } else if (ProtonCompositeBuffer.isComposite(payload)) {
            ((ProtonCompositeBuffer) payload).append(buffer);
        } else {
            this.payload = link.getEngine().configuration().getBufferAllocator().composite(new ProtonBuffer[] { payload, buffer });
        }

        return this;
    }

    @Override
    public String toString() {
        return "ProtonIncomingDelivery { " +
                    "deliveryId = " + deliveryId + ", " +
                    "deliveryTag = " + deliveryTag + " };";
    }
}

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

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonCompositeBuffer;
import org.apache.qpid.proton4j.engine.IncomingDelivery;
import org.apache.qpid.proton4j.types.DeliveryTag;
import org.apache.qpid.proton4j.types.transport.DeliveryState;

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

    private DeliveryState defaultDeliveryState;

    private DeliveryState localState;
    private boolean locallySettled;

    private DeliveryState remoteState;
    private boolean remotelySettled;

    private ProtonBuffer payload;
    private ProtonCompositeBuffer aggregate;

    private ProtonAttachments attachments;
    private Object linkedResource;

    /**
     * @param link
     *      The link that this delivery is associated with
     * @param deliveryId
     *      The Delivery Id that is assigned to this delivery.
     * @param deliveryTag
     *      The delivery tag assigned to this delivery
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
    public void setLinkedResource(Object resource) {
        this.linkedResource = resource;
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

    //----- Payload access

    @Override
    public int available() {
        return payload == null ? 0 : payload.getReadableBytes();
    }

    @Override
    public ProtonBuffer readAll() {
        ProtonBuffer result = null;
        if (payload != null) {
            int bytesRead = payload.getReadableBytes();
            result = payload;
            payload = null;
            aggregate = null;
            link.deliveryRead(this, bytesRead);
        }

        return result;
    }

    @Override
    public ProtonIncomingDelivery readBytes(ProtonBuffer buffer) {
        if (payload != null) {
            int bytesRead = payload.getReadableBytes();
            payload.readBytes(buffer);
            bytesRead -= payload.getReadableBytes();
            if (!payload.isReadable()) {
                payload = null;
                aggregate = null;
            }
            link.deliveryRead(this, bytesRead);
        }
        return this;
    }

    @Override
    public ProtonIncomingDelivery readBytes(byte[] array, int offset, int length) {
        if (payload != null) {
            int bytesRead = payload.getReadableBytes();
            payload.readBytes(array, offset, length);
            bytesRead -= payload.getReadableBytes();
            if (!payload.isReadable()) {
                payload = null;
                aggregate = null;
            }
            link.deliveryRead(this, bytesRead);
        }
        return this;
    }

    //----- Internal methods to manage the Delivery

    int getTransferCount() {
        return transferCount;
    }

    boolean isFirstTransfer() {
        return transferCount <= 1;
    }

    long getDeliveryId() {
        return deliveryId;
    }

    ProtonIncomingDelivery aborted() {
        aborted = true;

        if (payload != null) {
            final int bytesRead = payload.getReadableBytes();

            payload = null;
            aggregate = null;

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
            payload = buffer;
        } else if (aggregate != null) {
            aggregate.append(buffer);
        } else {
            final ProtonBuffer previous = payload;

            payload = aggregate = new ProtonCompositeBuffer();

            aggregate.append(previous);
            aggregate.append(buffer);
        }

        return this;
    }
}

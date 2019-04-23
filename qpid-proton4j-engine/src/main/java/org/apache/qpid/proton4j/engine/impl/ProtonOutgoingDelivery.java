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

import org.apache.qpid.proton4j.amqp.transport.DeliveryState;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.engine.Link;
import org.apache.qpid.proton4j.engine.OutgoingDelivery;
import org.apache.qpid.proton4j.engine.Sender;

/**
 * Proton outgoing delivery implementation
 */
public class ProtonOutgoingDelivery implements OutgoingDelivery {

    private final ProtonContext context = new ProtonContext();
    private final ProtonSender link;

    private byte[] deliveryTag;
    private boolean complete;
    private int messageFormat;
    private boolean aborted;

    private DeliveryState localState;
    private boolean locallySettled;
    // private boolean localSettleSent; // Track if settle was sent and is permanent.

    private DeliveryState remoteState;
    private boolean remotelySettled;

    private final ProtonBuffer payload = ProtonByteBufferAllocator.DEFAULT.allocate();

    /**
     * @param link
     *      The {@link Link} that is associated with this {@link OutgoingDelivery}
     */
    public ProtonOutgoingDelivery(ProtonSender link) {
        this.link = link;
    }

    @Override
    public Sender getLink() {
        return link;
    }

    @Override
    public ProtonContext getContext() {
        return context;
    }

    @Override
    public byte[] getTag() {
        return deliveryTag;
    }

    @Override
    public DeliveryState getLocalState() {
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
        this.localState = state;
        return this;
    }

    @Override
    public OutgoingDelivery disposition(DeliveryState state, boolean settle) {
        this.locallySettled = settle;
        this.localState = state;
        return this;
    }

    @Override
    public OutgoingDelivery settle() {
        this.locallySettled = true;
        return this;
    }

    @Override
    public void writeBytes(ProtonBuffer buffer) {
        checkCompleteOrAborted();
        payload.writeBytes(buffer);
        complete = true;
    }

    @Override
    public void streamBytes(ProtonBuffer buffer) {
        streamBytes(buffer, false);
    }

    @Override
    public void streamBytes(ProtonBuffer buffer, boolean complete) {
        checkCompleteOrAborted();
        payload.writeBytes(buffer);
        this.complete = complete;
    }

    @Override
    public OutgoingDelivery abort() {
        if (!aborted && !complete) {
            aborted = true;
            // TODO - Write transfer with aborted flag set
        }
        return this;
    }

    private void checkCompleteOrAborted() {
        if (complete || aborted) {
            throw new IllegalArgumentException("Cannot write to a delivery already marked as complete or has been aborted.");
        }
    }
}

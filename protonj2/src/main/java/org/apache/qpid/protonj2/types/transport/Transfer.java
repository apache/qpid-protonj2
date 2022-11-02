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
package org.apache.qpid.protonj2.types.transport;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.types.DeliveryTag;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;

public final class Transfer implements Performative {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000014L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:transfer:list");

    private static final long UINT_MAX = 0xFFFFFFFFL;

    private static final int HANDLE = 1;
    private static final int DELIVERY_ID = 2;
    private static final int DELIVERY_TAG = 4;
    private static final int MESSAGE_FORMAT = 8;
    private static final int SETTLED = 16;
    private static final int MORE = 32;
    private static final int RCV_SETTLE_MODE = 64;
    private static final int STATE = 128;
    private static final int RESUME = 256;
    private static final int ABORTED = 512;
    private static final int BATCHABLE = 1024;

    private int modified = 0;

    // TODO - Consider using the matching signed types instead of next largest
    //        for these values as in most cases we don't actually care about sign.
    //        In the cases we do care we could just do the math and make these
    //        interfaces simpler and not check all over the place for overflow.

    private long handle;
    private long deliveryId;
    private DeliveryTag deliveryTag;
    private long messageFormat;
    private boolean settled;
    private boolean more;
    private ReceiverSettleMode rcvSettleMode;
    private DeliveryState state;
    private boolean resume;
    private boolean aborted;
    private boolean batchable;

    //----- Query the state of the Header object -----------------------------//

    public boolean isEmpty() {
        return modified == 0;
    }

    public int getElementCount() {
        return 32 - Integer.numberOfLeadingZeros(modified);
    }

    public boolean hasElement(int index) {
        final int value = 1 << index;
        return (modified & value) == value;
    }

    public boolean hasHandle() {
        return (modified & HANDLE) == HANDLE;
    }

    public boolean hasDeliveryId() {
        return (modified & DELIVERY_ID) == DELIVERY_ID;
    }

    public boolean hasDeliveryTag() {
        return (modified & DELIVERY_TAG) == DELIVERY_TAG;
    }

    public boolean hasMessageFormat() {
        return (modified & MESSAGE_FORMAT) == MESSAGE_FORMAT;
    }

    public boolean hasSettled() {
        return (modified & SETTLED) == SETTLED;
    }

    public boolean hasMore() {
        return (modified & MORE) == MORE;
    }

    public boolean hasRcvSettleMode() {
        return (modified & RCV_SETTLE_MODE) == RCV_SETTLE_MODE;
    }

    public boolean hasState() {
        return (modified & STATE) == STATE;
    }

    public boolean hasResume() {
        return (modified & RESUME) == RESUME;
    }

    public boolean hasAborted() {
        return (modified & ABORTED) == ABORTED;
    }

    public boolean hasBatchable() {
        return (modified & BATCHABLE) == BATCHABLE;
    }

    //----- Access the AMQP Transfer object ------------------------------------//

    public long getHandle() {
        return handle;
    }

    public Transfer setHandle(int handle) {
        modified |= HANDLE;
        this.handle = Integer.toUnsignedLong(handle);
        return this;
    }

    public Transfer setHandle(long handle) {
        if (handle < 0 || handle > UINT_MAX) {
            throw new IllegalArgumentException("Handle value given is out of range: " + handle);
        } else {
            modified |= HANDLE;
        }

        this.handle = handle;
        return this;
    }

    public Transfer clearHandle() {
        modified &= ~HANDLE;
        handle = 0;
        return this;
    }

    public long getDeliveryId() {
        return deliveryId;
    }

    public Transfer setDeliveryId(int deliveryId) {
        modified |= DELIVERY_ID;
        this.deliveryId = Integer.toUnsignedLong(deliveryId);
        return this;
    }

    public Transfer setDeliveryId(long deliveryId) {
        if (deliveryId < 0 || deliveryId > UINT_MAX) {
            throw new IllegalArgumentException("Delivery ID value given is out of range: " + deliveryId);
        } else {
            modified |= DELIVERY_ID;
        }

        this.deliveryId = deliveryId;
        return this;
    }

    public Transfer clearDeliveryId() {
        modified &= ~DELIVERY_ID;
        deliveryId = 0;
        return this;
    }

    public DeliveryTag getDeliveryTag() {
        return deliveryTag;
    }

    public Transfer setDeliveryTag(byte[] tagBytes) {
        if (tagBytes != null) {
            return setDeliveryTag(new DeliveryTag.ProtonDeliveryTag(ProtonByteBufferAllocator.DEFAULT.wrap(tagBytes)));
        } else {
            return setDeliveryTag((DeliveryTag) null);
        }
    }

    public Transfer setDeliveryTag(ProtonBuffer tagBytes) {
        if (tagBytes != null) {
            return setDeliveryTag(new DeliveryTag.ProtonDeliveryTag(tagBytes));
        } else {
            return setDeliveryTag((DeliveryTag) null);
        }
    }

    public Transfer setDeliveryTag(DeliveryTag deliveryTag) {
        if (deliveryTag != null) {
            modified |= DELIVERY_TAG;
        } else {
            modified &= ~DELIVERY_TAG;
        }

        this.deliveryTag = deliveryTag;
        return this;
    }

    public Transfer clearDeliveryTag() {
        modified &= ~DELIVERY_TAG;
        deliveryTag = null;
        return this;
    }

    public long getMessageFormat() {
        return messageFormat;
    }

    public Transfer setMessageFormat(int messageFormat) {
        modified |= MESSAGE_FORMAT;
        this.messageFormat = Integer.toUnsignedLong(messageFormat);
        return this;
    }

    public Transfer setMessageFormat(long messageFormat) {
        if (messageFormat < 0 || messageFormat > UINT_MAX) {
            throw new IllegalArgumentException("Message Format value given is out of range: " + messageFormat);
        } else {
            modified |= MESSAGE_FORMAT;
        }

        this.messageFormat = messageFormat;
        return this;
    }

    public Transfer clearMessageFormat() {
        modified &= ~MESSAGE_FORMAT;
        messageFormat = 0;
        return this;
    }

    public boolean getSettled() {
        return settled;
    }

    public Transfer setSettled(boolean settled) {
        this.modified |= SETTLED;
        this.settled = settled;
        return this;
    }

    public Transfer clearSettled() {
        modified &= ~SETTLED;
        settled = false;
        return this;
    }

    public boolean getMore() {
        return more;
    }

    public Transfer setMore(boolean more) {
        this.modified |= MORE;
        this.more = more;
        return this;
    }

    public Transfer clearMore() {
        modified &= ~MORE;
        more = false;
        return this;
    }

    public ReceiverSettleMode getRcvSettleMode() {
        return rcvSettleMode;
    }

    public Transfer setRcvSettleMode(ReceiverSettleMode rcvSettleMode) {
        if (rcvSettleMode != null) {
            modified |= RCV_SETTLE_MODE;
        } else {
            modified &= ~RCV_SETTLE_MODE;
        }

        this.rcvSettleMode = rcvSettleMode;
        return this;
    }

    public Transfer clearRcvSettleMode() {
        modified &= ~RCV_SETTLE_MODE;
        rcvSettleMode = null;
        return this;
    }

    public DeliveryState getState() {
        return state;
    }

    public Transfer setState(DeliveryState state) {
        if (state != null) {
            modified |= STATE;
        } else {
            modified &= ~STATE;
        }

        this.state = state;
        return this;
    }

    public Transfer clearState() {
        modified &= ~STATE;
        state = null;
        return this;
    }

    public boolean getResume() {
        return resume;
    }

    public Transfer setResume(boolean resume) {
        this.modified |= RESUME;
        this.resume = resume;
        return this;
    }

    public Transfer clearResume() {
        modified &= ~RESUME;
        resume = false;
        return this;
    }

    public boolean getAborted() {
        return aborted;
    }

    public Transfer setAborted(boolean aborted) {
        this.modified |= ABORTED;
        this.aborted = aborted;
        return this;
    }

    public Transfer clearAborted() {
        modified &= ~ABORTED;
        aborted = false;
        return this;
    }

    public boolean getBatchable() {
        return batchable;
    }

    public Transfer setBatchable(boolean batchable) {
        this.modified |= BATCHABLE;
        this.batchable = batchable;
        return this;
    }

    public Transfer clearBatchable() {
        modified &= ~BATCHABLE;
        batchable = false;
        return this;
    }

    public Transfer reset() {
        modified = 0;
        handle = 0;
        deliveryId = 0;
        deliveryTag = null;
        messageFormat = 0;
        settled = false;
        more = false;
        rcvSettleMode = null;
        state = null;
        resume = false;
        aborted = false;
        batchable = false;

        return this;
    }

    @Override
    public Transfer copy() {
        Transfer copy = new Transfer();

        copy.handle = handle;
        copy.deliveryId = deliveryId;
        copy.deliveryTag = deliveryTag == null ? null : deliveryTag.copy();
        copy.messageFormat = messageFormat;
        copy.settled = settled;
        copy.more = more;
        copy.rcvSettleMode = rcvSettleMode;
        copy.state = state;
        copy.resume = resume;
        copy.aborted = aborted;
        copy.batchable = batchable;
        copy.modified = modified;

        return copy;
    }

    @Override
    public PerformativeType getPerformativeType() {
        return PerformativeType.TRANSFER;
    }

    @Override
    public <E> void invoke(PerformativeHandler<E> handler, ProtonBuffer payload, int channel, E context) {
        handler.handleTransfer(this, payload, channel, context);
    }

    @Override
    public String toString() {
        return "Transfer{" +
               "handle=" + (hasHandle() ? handle : "null") +
               ", deliveryId=" + (hasDeliveryId() ? deliveryId : "null") +
               ", deliveryTag=" + (hasDeliveryTag() ? deliveryTag : "null") +
               ", messageFormat=" + (hasMessageFormat() ? messageFormat : "null") +
               ", settled=" + (hasSettled() ? settled : "null") +
               ", more=" + (hasMore() ? more : "null") +
               ", rcvSettleMode=" + (hasRcvSettleMode() ? rcvSettleMode : "null") +
               ", state=" + (hasState() ? state : "null") +
               ", resume=" + (hasResume() ? resume : "null") +
               ", aborted=" + (hasAborted() ? aborted : "null") +
               ", batchable=" + (hasBatchable() ? batchable : "null") +
               '}';
    }
}

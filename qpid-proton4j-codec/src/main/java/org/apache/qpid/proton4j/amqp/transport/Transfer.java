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
package org.apache.qpid.proton4j.amqp.transport;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedLong;

public final class Transfer implements Performative {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000014L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:transfer:list");

    private static final long UINT_MAX = 0xFFFFFFFFL;

    private static int HANDLE = 1;
    private static int DELIVERY_ID = 2;
    private static int DELIVERY_TAG = 4;
    private static int MESSAGE_FORMAT = 8;
    private static int SETTLED = 16;
    private static int MORE = 32;
    private static int RCV_SETTLE_MODE = 64;
    private static int STATE = 128;
    private static int RESUME = 256;
    private static int ABORTED = 512;
    private static int BATCHABLE = 1024;

    private int modified = 0;

    private long handle;
    private long deliveryId;
    private Binary deliveryTag;
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

    public void setHandle(long handle) {
        if (handle < 0 || handle > UINT_MAX) {
            throw new IllegalArgumentException("Handle value given is out of range: " + handle);
        } else if (handle == 0) {
            modified &= ~HANDLE;
        } else {
            modified |= HANDLE;
        }

        this.handle = handle;
    }

    public long getDeliveryId() {
        return deliveryId;
    }

    public void setDeliveryId(long deliveryId) {
        if (deliveryId < 0 || deliveryId > UINT_MAX) {
            throw new IllegalArgumentException("Delivery ID value given is out of range: " + deliveryId);
        } else if (deliveryId == 0) {
            modified &= ~DELIVERY_ID;
        } else {
            modified |= DELIVERY_ID;
        }

        this.deliveryId = deliveryId;
    }

    public Binary getDeliveryTag() {
        return deliveryTag;
    }

    public void setDeliveryTag(Binary deliveryTag) {
        if (deliveryTag != null) {
            modified |= DELIVERY_TAG;
        } else {
            modified &= ~DELIVERY_TAG;
        }

        this.deliveryTag = deliveryTag;
    }

    public long getMessageFormat() {
        return messageFormat;
    }

    public void setMessageFormat(long messageFormat) {
        if (messageFormat < 0 || messageFormat > UINT_MAX) {
            throw new IllegalArgumentException("Message Format value given is out of range: " + messageFormat);
        } else if (messageFormat == 0) {
            modified &= ~MESSAGE_FORMAT;
        } else {
            modified |= MESSAGE_FORMAT;
        }

        this.messageFormat = messageFormat;
    }

    public boolean getSettled() {
        return settled;
    }

    public void setSettled(Boolean settled) {
        if (settled) {
            modified |= SETTLED;
        } else {
            modified &= ~SETTLED;
        }

        this.settled = settled;
    }

    public boolean getMore() {
        return more;
    }

    public void setMore(boolean more) {
        if (more) {
            modified |= MORE;
        } else {
            modified &= ~MORE;
        }

        this.more = more;
    }

    public ReceiverSettleMode getRcvSettleMode() {
        return rcvSettleMode;
    }

    public void setRcvSettleMode(ReceiverSettleMode rcvSettleMode) {
        if (rcvSettleMode != null) {
            modified |= RCV_SETTLE_MODE;
        } else {
            modified &= ~RCV_SETTLE_MODE;
        }

        this.rcvSettleMode = rcvSettleMode;
    }

    public DeliveryState getState() {
        return state;
    }

    public void setState(DeliveryState state) {
        if (state != null) {
            modified |= STATE;
        } else {
            modified &= ~STATE;
        }

        this.state = state;
    }

    public boolean getResume() {
        return resume;
    }

    public void setResume(boolean resume) {
        if (resume) {
            modified |= RESUME;
        } else {
            modified &= ~RESUME;
        }

        this.resume = resume;
    }

    public boolean getAborted() {
        return aborted;
    }

    public void setAborted(boolean aborted) {
        if (aborted) {
            modified |= ABORTED;
        } else {
            modified &= ~ABORTED;
        }

        this.aborted = aborted;
    }

    public boolean getBatchable() {
        return batchable;
    }

    public void setBatchable(boolean batchable) {
        if (batchable) {
            modified |= BATCHABLE;
        } else {
            modified &= ~BATCHABLE;
        }

        this.batchable = batchable;
    }

    @Override
    public PerformativeType getPerformativeType() {
        return PerformativeType.TRANSFER;
    }

    @Override
    public <E> void invoke(PerformativeHandler<E> handler, Binary payload, E context) {
        handler.handleTransfer(this, payload, context);
    }

    @Override
    public String toString() {
        return "Transfer{" +
               "handle=" + handle +
               ", deliveryId=" + deliveryId +
               ", deliveryTag=" + deliveryTag +
               ", messageFormat=" + messageFormat +
               ", settled=" + settled +
               ", more=" + more +
               ", rcvSettleMode=" + rcvSettleMode +
               ", state=" + state +
               ", resume=" + resume +
               ", aborted=" + aborted +
               ", batchable=" + batchable +
               '}';
    }
}

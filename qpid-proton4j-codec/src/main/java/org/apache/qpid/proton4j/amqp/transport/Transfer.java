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
import org.apache.qpid.proton4j.amqp.UnsignedInteger;

public final class Transfer {

    private UnsignedInteger handle;
    private UnsignedInteger deliveryId;
    private Binary deliveryTag;
    private UnsignedInteger messageFormat;
    private Boolean settled;
    private boolean more;
    private ReceiverSettleMode rcvSettleMode;
    private DeliveryState state;
    private boolean resume;
    private boolean aborted;
    private boolean batchable;

    public UnsignedInteger getHandle() {
        return handle;
    }

    public void setHandle(UnsignedInteger handle) {
        if (handle == null) {
            throw new NullPointerException("the handle field is mandatory");
        }

        this.handle = handle;
    }

    public UnsignedInteger getDeliveryId() {
        return deliveryId;
    }

    public void setDeliveryId(UnsignedInteger deliveryId) {
        this.deliveryId = deliveryId;
    }

    public Binary getDeliveryTag() {
        return deliveryTag;
    }

    public void setDeliveryTag(Binary deliveryTag) {
        this.deliveryTag = deliveryTag;
    }

    public UnsignedInteger getMessageFormat() {
        return messageFormat;
    }

    public void setMessageFormat(UnsignedInteger messageFormat) {
        this.messageFormat = messageFormat;
    }

    public Boolean getSettled() {
        return settled;
    }

    public void setSettled(Boolean settled) {
        this.settled = settled;
    }

    public boolean getMore() {
        return more;
    }

    public void setMore(boolean more) {
        this.more = more;
    }

    public ReceiverSettleMode getRcvSettleMode() {
        return rcvSettleMode;
    }

    public void setRcvSettleMode(ReceiverSettleMode rcvSettleMode) {
        this.rcvSettleMode = rcvSettleMode;
    }

    public DeliveryState getState() {
        return state;
    }

    public void setState(DeliveryState state) {
        this.state = state;
    }

    public boolean getResume() {
        return resume;
    }

    public void setResume(boolean resume) {
        this.resume = resume;
    }

    public boolean getAborted() {
        return aborted;
    }

    public void setAborted(boolean aborted) {
        this.aborted = aborted;
    }

    public boolean getBatchable() {
        return batchable;
    }

    public void setBatchable(boolean batchable) {
        this.batchable = batchable;
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

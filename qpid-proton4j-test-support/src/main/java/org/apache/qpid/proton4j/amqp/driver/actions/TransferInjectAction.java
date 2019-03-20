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
package org.apache.qpid.proton4j.amqp.driver.actions;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.transport.DeliveryState;
import org.apache.qpid.proton4j.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton4j.amqp.transport.Transfer;

/**
 * AMQP Close injection action which can be added to a driver for write at a specific time or
 * following on from some other action in the test script.
 */
public class TransferInjectAction extends AbstractPerformativeInjectAction<Transfer> {

    private final Transfer transfer;

    public TransferInjectAction(Transfer transfer) {
        this.transfer = transfer;
    }

    @Override
    public Transfer getPerformative() {
        return transfer;
    }

    public TransferInjectAction withHandle(long handle) {
        transfer.setHandle(handle);
        return this;
    }

    public TransferInjectAction withDeliveryId(long deliveryId) {
        transfer.setDeliveryId(deliveryId);
        return this;
    }

    public TransferInjectAction withDeliveryTag(Binary deliveryTag) {
        transfer.setDeliveryTag(deliveryTag);
        return this;
    }

    public TransferInjectAction withMessageFormat(long messageFormat) {
        transfer.setMessageFormat(messageFormat);
        return this;
    }

    public TransferInjectAction withSettled(boolean settled) {
        transfer.setSettled(settled);
        return this;
    }

    public TransferInjectAction withMore(boolean more) {
        transfer.setMore(more);
        return this;
    }

    public TransferInjectAction withRcvSettleMode(ReceiverSettleMode rcvSettleMode) {
        transfer.setRcvSettleMode(rcvSettleMode);
        return this;
    }

    public TransferInjectAction withState(DeliveryState state) {
        transfer.setState(state);
        return this;
    }

    public TransferInjectAction withResume(boolean resume) {
        transfer.setResume(resume);
        return this;
    }

    public TransferInjectAction withAborted(boolean aborted) {
        transfer.setAborted(aborted);
        return this;
    }

    public TransferInjectAction withBatchable(boolean batchable) {
        transfer.setBatchable(batchable);
        return this;
    }
}

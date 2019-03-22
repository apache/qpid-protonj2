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
package org.apache.qpid.proton4j.amqp.driver.expectations;

import static org.hamcrest.CoreMatchers.equalTo;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.amqp.transport.DeliveryState;
import org.apache.qpid.proton4j.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton4j.amqp.transport.Transfer;
import org.hamcrest.Matcher;

/**
 * Scripted expectation for the AMQP Transfer performative
 */
public class TransferExpectation extends AbstractExpectation<Transfer> {

    /**
     * Enumeration which maps to fields in the Transfer Performative
     */
    public enum Field {
        HANDLE,
        DELIVERY_ID,
        DELIVERY_TAG,
        MESSAGE_FORMAT,
        SETTLED,
        MORE,
        RCV_SETTLE_MODE,
        STATE,
        RESUME,
        ABORTED,
        BATCHABLE
    }

    public TransferExpectation(AMQPTestDriver driver) {
        super(driver);
    }

    @Override
    public TransferExpectation onChannel(int channel) {
        super.onChannel(channel);
        return this;
    }

    //----- Type specific with methods that perform simple equals checks

    public TransferExpectation withHandle(long handle) {
        return withHandle(equalTo(handle));
    }

    public TransferExpectation withDeliveryId(long deliveryId) {
        return withDeliveryId(equalTo(deliveryId));
    }

    public TransferExpectation withDeliveryTag(Binary deliveryTag) {
        return withDeliveryTag(equalTo(deliveryTag));
    }

    public TransferExpectation withMessageFormat(long messageFormat) {
        return withMessageFormat(equalTo(messageFormat));
    }

    public TransferExpectation withSettled(boolean settled) {
        return withSettled(equalTo(settled));
    }

    public TransferExpectation withMore(boolean more) {
        return withMore(equalTo(more));
    }

    public TransferExpectation withRcvSettleMode(ReceiverSettleMode rcvSettleMode) {
        return withRcvSettleMode(equalTo(rcvSettleMode));
    }

    public TransferExpectation withState(DeliveryState state) {
        return withState(equalTo(state));
    }

    public TransferExpectation withResume(boolean resume) {
        return withResume(equalTo(resume));
    }

    public TransferExpectation withAborted(boolean aborted) {
        return withAborted(equalTo(aborted));
    }

    public TransferExpectation withBatchable(boolean batchable) {
        return withBatchable(equalTo(batchable));
    }

    //----- Matcher based with methods for more complex validation

    public TransferExpectation withHandle(Matcher<?> m) {
        getMatchers().put(Field.HANDLE, m);
        return this;
    }

    public TransferExpectation withDeliveryId(Matcher<?> m) {
        getMatchers().put(Field.DELIVERY_ID, m);
        return this;
    }

    public TransferExpectation withDeliveryTag(Matcher<?> m) {
        getMatchers().put(Field.DELIVERY_TAG, m);
        return this;
    }

    public TransferExpectation withMessageFormat(Matcher<?> m) {
        getMatchers().put(Field.MESSAGE_FORMAT, m);
        return this;
    }

    public TransferExpectation withSettled(Matcher<?> m) {
        getMatchers().put(Field.SETTLED, m);
        return this;
    }

    public TransferExpectation withMore(Matcher<?> m) {
        getMatchers().put(Field.MORE, m);
        return this;
    }

    public TransferExpectation withRcvSettleMode(Matcher<?> m) {
        getMatchers().put(Field.RCV_SETTLE_MODE, m);
        return this;
    }

    public TransferExpectation withState(Matcher<?> m) {
        getMatchers().put(Field.STATE, m);
        return this;
    }

    public TransferExpectation withResume(Matcher<?> m) {
        getMatchers().put(Field.RESUME, m);
        return this;
    }

    public TransferExpectation withAborted(Matcher<?> m) {
        getMatchers().put(Field.ABORTED, m);
        return this;
    }

    public TransferExpectation withBatchable(Matcher<?> m) {
        getMatchers().put(Field.BATCHABLE, m);
        return this;
    }

    @Override
    protected Object getFieldValue(Transfer transfer, Enum<?> performativeField) {
        Object result = null;

        if (performativeField == Field.HANDLE) {
            result = transfer.hasHandle() ? transfer.getHandle() : null;
        } else if (performativeField == Field.DELIVERY_ID) {
            result = transfer.hasDeliveryId() ? transfer.getDeliveryId() : null;
        } else if (performativeField == Field.DELIVERY_TAG) {
            result = transfer.hasDeliveryTag() ? transfer.getDeliveryTag() : null;
        } else if (performativeField == Field.MESSAGE_FORMAT) {
            result = transfer.hasMessageFormat() ? transfer.getMessageFormat() : null;
        } else if (performativeField == Field.SETTLED) {
            result = transfer.hasSettled() ? transfer.getSettled() : null;
        } else if (performativeField == Field.MORE) {
            result = transfer.hasMore() ? transfer.getMore() : null;
        } else if (performativeField == Field.RCV_SETTLE_MODE) {
            result = transfer.hasRcvSettleMode() ? transfer.getRcvSettleMode() : null;
        } else if (performativeField == Field.STATE) {
            result = transfer.hasState() ? transfer.getState() : null;
        } else if (performativeField == Field.RESUME) {
            result = transfer.hasResume() ? transfer.getResume() : null;
        } else if (performativeField == Field.ABORTED) {
            result = transfer.hasAborted() ? transfer.getAborted() : null;
        } else if (performativeField == Field.BATCHABLE) {
            result = transfer.hasBatchable() ? transfer.getBatchable() : null;
        } else {
            throw new AssertionError("Request for unknown field in type Transfer");
        }

        return result;
    }

    @Override
    protected Enum<?> getFieldEnum(int fieldIndex) {
        return Field.values()[fieldIndex];
    }

    @Override
    protected Class<Transfer> getExpectedTypeClass() {
        return Transfer.class;
    }
}

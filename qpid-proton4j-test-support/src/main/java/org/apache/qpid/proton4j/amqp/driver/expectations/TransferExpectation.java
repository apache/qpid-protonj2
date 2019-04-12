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
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.amqp.driver.codec.security.SaslResponse;
import org.apache.qpid.proton4j.amqp.driver.codec.transport.Transfer;
import org.apache.qpid.proton4j.amqp.driver.codec.util.TypeMapper;
import org.apache.qpid.proton4j.amqp.transport.DeliveryState;
import org.apache.qpid.proton4j.amqp.transport.ReceiverSettleMode;
import org.hamcrest.Matcher;

/**
 * Scripted expectation for the AMQP Transfer performative
 */
public class TransferExpectation extends AbstractExpectation<Transfer> {

    public TransferExpectation(AMQPTestDriver driver) {
        super(driver);
    }

    @Override
    public TransferExpectation onChannel(int channel) {
        super.onChannel(channel);
        return this;
    }

    //----- Type specific with methods that perform simple equals checks

    public TransferExpectation withHandle(int handle) {
        return withHandle(equalTo(UnsignedInteger.valueOf(handle)));
    }

    public TransferExpectation withHandle(long handle) {
        return withHandle(equalTo(UnsignedInteger.valueOf(handle)));
    }

    public TransferExpectation withHandle(UnsignedInteger handle) {
        return withHandle(equalTo(handle));
    }

    public TransferExpectation withDeliveryId(int deliveryId) {
        return withDeliveryId(equalTo(UnsignedInteger.valueOf(deliveryId)));
    }

    public TransferExpectation withDeliveryId(long deliveryId) {
        return withDeliveryId(equalTo(UnsignedInteger.valueOf(deliveryId)));
    }

    public TransferExpectation withDeliveryId(UnsignedInteger deliveryId) {
        return withDeliveryId(equalTo(deliveryId));
    }

    public TransferExpectation withDeliveryTag(Binary deliveryTag) {
        return withDeliveryTag(equalTo(deliveryTag));
    }

    public TransferExpectation withMessageFormat(int messageFormat) {
        return withMessageFormat(equalTo(UnsignedInteger.valueOf(messageFormat)));
    }

    public TransferExpectation withMessageFormat(long messageFormat) {
        return withMessageFormat(equalTo(UnsignedInteger.valueOf(messageFormat)));
    }

    public TransferExpectation withMessageFormat(UnsignedInteger messageFormat) {
        return withMessageFormat(equalTo(messageFormat));
    }

    public TransferExpectation withSettled(boolean settled) {
        return withSettled(equalTo(settled));
    }

    public TransferExpectation withMore(boolean more) {
        return withMore(equalTo(more));
    }

    public TransferExpectation withRcvSettleMode(ReceiverSettleMode rcvSettleMode) {
        return withRcvSettleMode(equalTo(rcvSettleMode.getValue()));
    }

    public TransferExpectation withState(DeliveryState state) {
        return withState(equalTo(TypeMapper.mapFromProtonType(state)));
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
        getMatchers().put(Transfer.Field.HANDLE, m);
        return this;
    }

    public TransferExpectation withDeliveryId(Matcher<?> m) {
        getMatchers().put(Transfer.Field.DELIVERY_ID, m);
        return this;
    }

    public TransferExpectation withDeliveryTag(Matcher<?> m) {
        getMatchers().put(Transfer.Field.DELIVERY_TAG, m);
        return this;
    }

    public TransferExpectation withMessageFormat(Matcher<?> m) {
        getMatchers().put(Transfer.Field.MESSAGE_FORMAT, m);
        return this;
    }

    public TransferExpectation withSettled(Matcher<?> m) {
        getMatchers().put(Transfer.Field.SETTLED, m);
        return this;
    }

    public TransferExpectation withMore(Matcher<?> m) {
        getMatchers().put(Transfer.Field.MORE, m);
        return this;
    }

    public TransferExpectation withRcvSettleMode(Matcher<?> m) {
        getMatchers().put(Transfer.Field.RCV_SETTLE_MODE, m);
        return this;
    }

    public TransferExpectation withState(Matcher<?> m) {
        getMatchers().put(Transfer.Field.STATE, m);
        return this;
    }

    public TransferExpectation withResume(Matcher<?> m) {
        getMatchers().put(Transfer.Field.RESUME, m);
        return this;
    }

    public TransferExpectation withAborted(Matcher<?> m) {
        getMatchers().put(Transfer.Field.ABORTED, m);
        return this;
    }

    public TransferExpectation withBatchable(Matcher<?> m) {
        getMatchers().put(Transfer.Field.BATCHABLE, m);
        return this;
    }

    @Override
    protected Object getFieldValue(Transfer transfer, Enum<?> performativeField) {
        return transfer.getFieldValue(performativeField.ordinal());
    }

    @Override
    protected Enum<?> getFieldEnum(int fieldIndex) {
        return SaslResponse.Field.values()[fieldIndex];
    }

    @Override
    protected Class<Transfer> getExpectedTypeClass() {
        return Transfer.class;
    }
}

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
package org.apache.qpid.proton4j.test.driver.matchers.transport;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;

import org.apache.qpid.proton4j.test.driver.codec.primitives.Binary;
import org.apache.qpid.proton4j.test.driver.codec.primitives.UnsignedInteger;
import org.apache.qpid.proton4j.test.driver.codec.transport.DeliveryState;
import org.apache.qpid.proton4j.test.driver.codec.transport.ReceiverSettleMode;
import org.apache.qpid.proton4j.test.driver.codec.transport.Transfer;
import org.apache.qpid.proton4j.test.driver.matchers.ListDescribedTypeMatcher;
import org.hamcrest.Matcher;

public class TransferMatcher extends ListDescribedTypeMatcher {

    public TransferMatcher() {
        super(Transfer.Field.values().length, Transfer.DESCRIPTOR_CODE, Transfer.DESCRIPTOR_SYMBOL);
    }

    @Override
    protected Class<?> getDescribedTypeClass() {
        return Transfer.class;
    }

    //----- Type specific with methods that perform simple equals checks

    public TransferMatcher withHandle(int handle) {
        return withHandle(equalTo(UnsignedInteger.valueOf(handle)));
    }

    public TransferMatcher withHandle(long handle) {
        return withHandle(equalTo(UnsignedInteger.valueOf(handle)));
    }

    public TransferMatcher withHandle(UnsignedInteger handle) {
        return withHandle(equalTo(handle));
    }

    public TransferMatcher withDeliveryId(int deliveryId) {
        return withDeliveryId(equalTo(UnsignedInteger.valueOf(deliveryId)));
    }

    public TransferMatcher withDeliveryId(long deliveryId) {
        return withDeliveryId(equalTo(UnsignedInteger.valueOf(deliveryId)));
    }

    public TransferMatcher withDeliveryId(UnsignedInteger deliveryId) {
        return withDeliveryId(equalTo(deliveryId));
    }

    public TransferMatcher withDeliveryTag(byte[] tag) {
        return withDeliveryTag(new Binary(tag));
    }

    public TransferMatcher withDeliveryTag(Binary deliveryTag) {
        return withDeliveryTag(equalTo(deliveryTag));
    }

    public TransferMatcher withMessageFormat(int messageFormat) {
        return withMessageFormat(equalTo(UnsignedInteger.valueOf(messageFormat)));
    }

    public TransferMatcher withMessageFormat(long messageFormat) {
        return withMessageFormat(equalTo(UnsignedInteger.valueOf(messageFormat)));
    }

    public TransferMatcher withMessageFormat(UnsignedInteger messageFormat) {
        return withMessageFormat(equalTo(messageFormat));
    }

    public TransferMatcher withSettled(boolean settled) {
        return withSettled(equalTo(settled));
    }

    public TransferMatcher withMore(boolean more) {
        return withMore(equalTo(more));
    }

    public TransferMatcher withRcvSettleMode(ReceiverSettleMode rcvSettleMode) {
        return withRcvSettleMode(equalTo(rcvSettleMode.getValue()));
    }

    public TransferMatcher withState(DeliveryState state) {
        return withState(equalTo(state));
    }

    public TransferMatcher withNullState() {
        return withState(nullValue());
    }

    public TransferMatcher withResume(boolean resume) {
        return withResume(equalTo(resume));
    }

    public TransferMatcher withAborted(boolean aborted) {
        return withAborted(equalTo(aborted));
    }

    public TransferMatcher withBatchable(boolean batchable) {
        return withBatchable(equalTo(batchable));
    }

    //----- Matcher based with methods for more complex validation

    public TransferMatcher withHandle(Matcher<?> m) {
        addFieldMatcher(Transfer.Field.HANDLE, m);
        return this;
    }

    public TransferMatcher withDeliveryId(Matcher<?> m) {
        addFieldMatcher(Transfer.Field.DELIVERY_ID, m);
        return this;
    }

    public TransferMatcher withDeliveryTag(Matcher<?> m) {
        addFieldMatcher(Transfer.Field.DELIVERY_TAG, m);
        return this;
    }

    public TransferMatcher withMessageFormat(Matcher<?> m) {
        addFieldMatcher(Transfer.Field.MESSAGE_FORMAT, m);
        return this;
    }

    public TransferMatcher withSettled(Matcher<?> m) {
        addFieldMatcher(Transfer.Field.SETTLED, m);
        return this;
    }

    public TransferMatcher withMore(Matcher<?> m) {
        addFieldMatcher(Transfer.Field.MORE, m);
        return this;
    }

    public TransferMatcher withRcvSettleMode(Matcher<?> m) {
        addFieldMatcher(Transfer.Field.RCV_SETTLE_MODE, m);
        return this;
    }

    public TransferMatcher withState(Matcher<?> m) {
        addFieldMatcher(Transfer.Field.STATE, m);
        return this;
    }

    public TransferMatcher withResume(Matcher<?> m) {
        addFieldMatcher(Transfer.Field.RESUME, m);
        return this;
    }

    public TransferMatcher withAborted(Matcher<?> m) {
        addFieldMatcher(Transfer.Field.ABORTED, m);
        return this;
    }

    public TransferMatcher withBatchable(Matcher<?> m) {
        addFieldMatcher(Transfer.Field.BATCHABLE, m);
        return this;
    }
}

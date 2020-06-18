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
package org.apache.qpid.proton4j.test.driver.expectations;

import static org.hamcrest.CoreMatchers.equalTo;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.test.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.test.driver.LinkTracker;
import org.apache.qpid.proton4j.test.driver.actions.BeginInjectAction;
import org.apache.qpid.proton4j.test.driver.actions.DispositionInjectAction;
import org.apache.qpid.proton4j.test.driver.codec.ListDescribedType;
import org.apache.qpid.proton4j.test.driver.codec.security.SaslResponse;
import org.apache.qpid.proton4j.test.driver.codec.transport.Transfer;
import org.apache.qpid.proton4j.test.driver.codec.util.TypeMapper;
import org.apache.qpid.proton4j.test.driver.matchers.transport.TransferMatcher;
import org.apache.qpid.proton4j.types.Binary;
import org.apache.qpid.proton4j.types.Symbol;
import org.apache.qpid.proton4j.types.UnsignedInteger;
import org.apache.qpid.proton4j.types.messaging.Accepted;
import org.apache.qpid.proton4j.types.messaging.Modified;
import org.apache.qpid.proton4j.types.messaging.Rejected;
import org.apache.qpid.proton4j.types.messaging.Released;
import org.apache.qpid.proton4j.types.transport.DeliveryState;
import org.apache.qpid.proton4j.types.transport.ErrorCondition;
import org.apache.qpid.proton4j.types.transport.ReceiverSettleMode;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

/**
 * Scripted expectation for the AMQP Transfer performative
 */
public class TransferExpectation extends AbstractExpectation<Transfer> {

    private final TransferMatcher matcher = new TransferMatcher();

    private Matcher<ProtonBuffer> payloadMatcher = Matchers.any(ProtonBuffer.class);

    protected DispositionInjectAction response;

    public TransferExpectation(AMQPTestDriver driver) {
        super(driver);
    }

    public DispositionInjectAction respond() {
        response = new DispositionInjectAction(driver);
        driver.addScriptedElement(response);
        return response;
    }

    public DispositionInjectAction accept() {
        response = new DispositionInjectAction(driver);
        response.withSettled(true);
        response.withState(Accepted.getInstance());

        driver.addScriptedElement(response);
        return response;
    }

    public DispositionInjectAction release() {
        response = new DispositionInjectAction(driver);
        response.withSettled(true);
        response.withState(Released.getInstance());

        driver.addScriptedElement(response);
        return response;
    }

    public DispositionInjectAction reject() {
        return reject(null);
    }

    public DispositionInjectAction reject(String condition, String description) {
        return reject(new ErrorCondition(Symbol.valueOf(condition), description));
    }

    public DispositionInjectAction reject(Symbol condition, String description) {
        return reject(new ErrorCondition(condition, description));
    }

    public DispositionInjectAction reject(ErrorCondition error) {
        response = new DispositionInjectAction(driver);
        response.withSettled(true);
        response.withState(new Rejected().setError(error));

        driver.addScriptedElement(response);
        return response;
    }

    public DispositionInjectAction modify(boolean failed) {
        return modify(failed, false);
    }

    public DispositionInjectAction modify(boolean failed, boolean undeliverable) {
        response = new DispositionInjectAction(driver);
        response.withSettled(true);
        response.withState(new Modified().setDeliveryFailed(failed).setUndeliverableHere(undeliverable));

        driver.addScriptedElement(response);
        return response;
    }

    @Override
    public TransferExpectation onChannel(int channel) {
        super.onChannel(channel);
        return this;
    }

    @Override
    public void handleTransfer(Transfer transfer, ProtonBuffer payload, int channel, AMQPTestDriver driver) {
        super.handleTransfer(transfer, payload, channel, driver);

        if (response == null) {
            return;
        }

        LinkTracker link = driver.getSessions().handleTransfer(transfer, payload, channel);

        // Input was validated now populate response with auto values where not configured
        // to say otherwise by the test.
        if (response.onChannel() == BeginInjectAction.CHANNEL_UNSET) {
            response.onChannel(link.getSession().getLocalChannel());
        }

        // Populate the fields of the response with defaults if non set by the test script
        if (response.getPerformative().getFirst() == null) {
            response.withFirst(transfer.getDeliveryId());
        }

        if (response.getPerformative().getRole() == null) {
            response.withRole(link.getRole());
        }

        // Remaining response fields should be set by the test script as they can't be inferred.
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

    public TransferExpectation withDeliveryTag(byte[] tag) {
        return withDeliveryTag(new Binary(tag));
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

    public TransferExpectation withPayload(byte[] buffer) {
        // TODO - Create Matcher which describes the mismatch in detail
        this.payloadMatcher = Matchers.equalTo(ProtonByteBufferAllocator.DEFAULT.wrap(buffer));
        return this;
    }

    public TransferExpectation withPayload(ProtonBuffer buffer) {
        // TODO - Create Matcher which describes the mismatch in detail
        this.payloadMatcher = Matchers.equalTo(buffer);
        return this;
    }

    //----- Matcher based with methods for more complex validation

    public TransferExpectation withHandle(Matcher<?> m) {
        matcher.addFieldMatcher(Transfer.Field.HANDLE, m);
        return this;
    }

    public TransferExpectation withDeliveryId(Matcher<?> m) {
        matcher.addFieldMatcher(Transfer.Field.DELIVERY_ID, m);
        return this;
    }

    public TransferExpectation withDeliveryTag(Matcher<?> m) {
        matcher.addFieldMatcher(Transfer.Field.DELIVERY_TAG, m);
        return this;
    }

    public TransferExpectation withMessageFormat(Matcher<?> m) {
        matcher.addFieldMatcher(Transfer.Field.MESSAGE_FORMAT, m);
        return this;
    }

    public TransferExpectation withSettled(Matcher<?> m) {
        matcher.addFieldMatcher(Transfer.Field.SETTLED, m);
        return this;
    }

    public TransferExpectation withMore(Matcher<?> m) {
        matcher.addFieldMatcher(Transfer.Field.MORE, m);
        return this;
    }

    public TransferExpectation withRcvSettleMode(Matcher<?> m) {
        matcher.addFieldMatcher(Transfer.Field.RCV_SETTLE_MODE, m);
        return this;
    }

    public TransferExpectation withState(Matcher<?> m) {
        matcher.addFieldMatcher(Transfer.Field.STATE, m);
        return this;
    }

    public TransferExpectation withResume(Matcher<?> m) {
        matcher.addFieldMatcher(Transfer.Field.RESUME, m);
        return this;
    }

    public TransferExpectation withAborted(Matcher<?> m) {
        matcher.addFieldMatcher(Transfer.Field.ABORTED, m);
        return this;
    }

    public TransferExpectation withBatchable(Matcher<?> m) {
        matcher.addFieldMatcher(Transfer.Field.BATCHABLE, m);
        return this;
    }

    public TransferExpectation withPayload(Matcher<ProtonBuffer> payloadMatcher) {
        this.payloadMatcher = payloadMatcher;
        return this;
    }

    @Override
    protected Matcher<ListDescribedType> getExpectationMatcher() {
        return matcher;
    }

    @Override
    protected Matcher<ProtonBuffer> getPayloadMatcher() {
        return payloadMatcher;
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

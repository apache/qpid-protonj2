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
import static org.hamcrest.CoreMatchers.nullValue;

import java.util.Map;
import java.util.UUID;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.amqp.driver.LinkTracker;
import org.apache.qpid.proton4j.amqp.driver.actions.AttachInjectAction;
import org.apache.qpid.proton4j.amqp.driver.actions.BeginInjectAction;
import org.apache.qpid.proton4j.amqp.driver.codec.ListDescribedType;
import org.apache.qpid.proton4j.amqp.driver.codec.transport.Attach;
import org.apache.qpid.proton4j.amqp.driver.codec.util.TypeMapper;
import org.apache.qpid.proton4j.amqp.driver.matchers.messaging.SourceMatcher;
import org.apache.qpid.proton4j.amqp.driver.matchers.messaging.TargetMatcher;
import org.apache.qpid.proton4j.amqp.driver.matchers.transactions.CoordinatorMatcher;
import org.apache.qpid.proton4j.amqp.driver.matchers.transport.AttachMatcher;
import org.apache.qpid.proton4j.amqp.messaging.Source;
import org.apache.qpid.proton4j.amqp.messaging.Target;
import org.apache.qpid.proton4j.amqp.messaging.TerminusDurability;
import org.apache.qpid.proton4j.amqp.messaging.TerminusExpiryPolicy;
import org.apache.qpid.proton4j.amqp.transactions.Coordinator;
import org.apache.qpid.proton4j.amqp.transport.DeliveryState;
import org.apache.qpid.proton4j.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton4j.amqp.transport.Role;
import org.apache.qpid.proton4j.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.hamcrest.Matcher;

/**
 * Scripted expectation for the AMQP Attach performative
 */
public class AttachExpectation extends AbstractExpectation<Attach> {

    private final AttachMatcher matcher = new AttachMatcher();

    private AttachInjectAction response;

    public AttachExpectation(AMQPTestDriver driver) {
        super(driver);
    }

    @Override
    public AttachExpectation onChannel(int channel) {
        super.onChannel(channel);
        return this;
    }

    public AttachInjectAction respond() {
        response = new AttachInjectAction(driver);
        driver.addScriptedElement(response);
        return response;
    }

    //----- Handle the performative and configure response is told to respond

    @Override
    public void handleAttach(Attach attach, ProtonBuffer payload, int channel, AMQPTestDriver context) {
        super.handleAttach(attach, payload, channel, context);

        LinkTracker link = driver.getSessions().handleAttach(attach, channel);

        if (response == null) {
            return;
        }

        // Input was validated now populate response with auto values where not configured
        // to say otherwise by the test.
        if (response.onChannel() == BeginInjectAction.CHANNEL_UNSET) {
            response.onChannel(link.getSession().getLocalChannel());
        }

        // Populate the fields of the response with defaults if non set by the test script
        if (response.getPerformative().getHandle() == null) {
            response.withHandle(attach.getHandle());
        }
        if (response.getPerformative().getName() == null) {
            response.withName(attach.getName());
        }
        if (response.getPerformative().getRole() == null) {
            response.withRole(Boolean.TRUE.equals(attach.getRole()) ? Role.SENDER : Role.RECEIVER);
        }
        if (response.getPerformative().getSndSettleMode() == null) {
            response.withSndSettleMode(SenderSettleMode.valueOf(attach.getSndSettleMode()));
        }
        if (response.getPerformative().getRcvSettleMode() == null) {
            response.withRcvSettleMode(ReceiverSettleMode.valueOf(attach.getRcvSettleMode()));
        }
        if (response.getPerformative().getSource() == null && !response.isNullSourceRequired()) {
            response.withSource(attach.getSource());
            if (attach.getSource() != null && Boolean.TRUE.equals(attach.getSource().getDynamic())) {
                attach.getSource().setAddress(UUID.randomUUID().toString());
            }
        }
        if (response.getPerformative().getTarget() == null && !response.isNullTargetRequired()) {
            if (attach.getTarget() != null) {
                if (attach.getTarget() instanceof org.apache.qpid.proton4j.amqp.driver.codec.messaging.Target) {
                    org.apache.qpid.proton4j.amqp.driver.codec.messaging.Target target =
                        (org.apache.qpid.proton4j.amqp.driver.codec.messaging.Target) attach.getTarget();
                    response.withTarget(target);
                    if (target != null && Boolean.TRUE.equals(target.getDynamic())) {
                        target.setAddress(UUID.randomUUID().toString());
                    }
                } else {
                    org.apache.qpid.proton4j.amqp.driver.codec.transactions.Coordinator coordinator =
                        (org.apache.qpid.proton4j.amqp.driver.codec.transactions.Coordinator) attach.getTarget();
                    response.withTarget(coordinator);
                }
            }
        }

        if (response.getPerformative().getInitialDeliveryCount() == null) {
            Role role = Role.valueOf(response.getPerformative().getRole());
            if (role == Role.SENDER) {
                response.withInitialDeliveryCount(0);
            }
        }

        // Other fields are left not set for now unless test script configured
    }

    //----- Type specific with methods that perform simple equals checks

    public AttachExpectation withName(String name) {
        return withName(equalTo(name));
    }

    public AttachExpectation withHandle(int handle) {
        return withHandle(equalTo(UnsignedInteger.valueOf(handle)));
    }

    public AttachExpectation withHandle(long handle) {
        return withHandle(equalTo(UnsignedInteger.valueOf(handle)));
    }

    public AttachExpectation withHandle(UnsignedInteger handle) {
        return withHandle(equalTo(handle));
    }

    public AttachExpectation withRole(Role role) {
        return withRole(equalTo(role.getValue()));
    }

    public AttachExpectation withSndSettleMode(SenderSettleMode sndSettleMode) {
        return withSndSettleMode(sndSettleMode == null ? nullValue() : equalTo(sndSettleMode.getValue()));
    }

    public AttachExpectation withRcvSettleMode(ReceiverSettleMode rcvSettleMode) {
        return withRcvSettleMode(rcvSettleMode == null ? nullValue() : equalTo(rcvSettleMode.getValue()));
    }

    public AttachSourceMatcher withSource() {
        AttachSourceMatcher matcher = new AttachSourceMatcher(this);
        withSource(matcher);
        return matcher;
    }

    public AttachTargetMatcher withTarget() {
        AttachTargetMatcher matcher = new AttachTargetMatcher(this);
        withTarget(matcher);
        return matcher;
    }

    public AttachExpectation withSource(Source source) {
        if (source != null) {
            SourceMatcher sourceMatcher = new SourceMatcher(source);
            return withSource(sourceMatcher);
        } else {
            return withSource(nullValue());
        }
    }

    public AttachExpectation withTarget(Target target) {
        if (target != null) {
            TargetMatcher targetMatcher = new TargetMatcher(target);
            return withTarget(targetMatcher);
        } else {
            return withTarget(nullValue());
        }
    }

    public AttachExpectation withTarget(Coordinator coordinator) {
        if (coordinator != null) {
            CoordinatorMatcher coordinatorMatcher = new CoordinatorMatcher();
            return withTarget(coordinatorMatcher);
        } else {
            return withTarget(nullValue());
        }
    }

    public AttachExpectation withUnsettled(Map<Binary, DeliveryState> unsettled) {
        // TODO - Need to match on the driver types for DeliveryState
        return withUnsettled(equalTo(unsettled));
    }

    public AttachExpectation withIncompleteUnsettled(boolean incomplete) {
        return withIncompleteUnsettled(equalTo(incomplete));
    }

    public AttachExpectation withInitialDeliveryCount(int initialDeliveryCount) {
        return withInitialDeliveryCount(equalTo(UnsignedInteger.valueOf(initialDeliveryCount)));
    }

    public AttachExpectation withInitialDeliveryCount(long initialDeliveryCount) {
        return withInitialDeliveryCount(equalTo(UnsignedInteger.valueOf(initialDeliveryCount)));
    }

    public AttachExpectation withInitialDeliveryCount(UnsignedInteger initialDeliveryCount) {
        return withInitialDeliveryCount(equalTo(initialDeliveryCount));
    }

    public AttachExpectation withMaxMessageSize(long maxMessageSize) {
        return withMaxMessageSize(equalTo(UnsignedLong.valueOf(maxMessageSize)));
    }

    public AttachExpectation withMaxMessageSize(UnsignedLong maxMessageSize) {
        return withMaxMessageSize(equalTo(maxMessageSize));
    }

    public AttachExpectation withOfferedCapabilities(Symbol... offeredCapabilities) {
        return withOfferedCapabilities(equalTo(offeredCapabilities));
    }

    public AttachExpectation withDesiredCapabilities(Symbol... desiredCapabilities) {
        return withDesiredCapabilities(equalTo(desiredCapabilities));
    }

    public AttachExpectation withProperties(Map<Symbol, Object> properties) {
        return withProperties(equalTo(properties));
    }

    public AttachExpectation withPropertiesMap(Map<String, Object> properties) {
        return withProperties(equalTo(TypeMapper.toSymbolKeyedMap(properties)));
    }

    //----- Matcher based with methods for more complex validation

    public AttachExpectation withName(Matcher<?> m) {
        matcher.addFieldMatcher(Attach.Field.NAME, m);
        return this;
    }

    public AttachExpectation withHandle(Matcher<?> m) {
        matcher.addFieldMatcher(Attach.Field.HANDLE, m);
        return this;
    }

    public AttachExpectation withRole(Matcher<?> m) {
        matcher.addFieldMatcher(Attach.Field.ROLE, m);
        return this;
    }

    public AttachExpectation withSndSettleMode(Matcher<?> m) {
        matcher.addFieldMatcher(Attach.Field.SND_SETTLE_MODE, m);
        return this;
    }

    public AttachExpectation withRcvSettleMode(Matcher<?> m) {
        matcher.addFieldMatcher(Attach.Field.RCV_SETTLE_MODE, m);
        return this;
    }

    public AttachExpectation withSource(Matcher<?> m) {
        matcher.addFieldMatcher(Attach.Field.SOURCE, m);
        return this;
    }

    public AttachExpectation withTarget(Matcher<?> m) {
        matcher.addFieldMatcher(Attach.Field.TARGET, m);
        return this;
    }

    public AttachExpectation withUnsettled(Matcher<?> m) {
        matcher.addFieldMatcher(Attach.Field.UNSETTLED, m);
        return this;
    }

    public AttachExpectation withIncompleteUnsettled(Matcher<?> m) {
        matcher.addFieldMatcher(Attach.Field.INCOMPLETE_UNSETTLED, m);
        return this;
    }

    public AttachExpectation withInitialDeliveryCount(Matcher<?> m) {
        matcher.addFieldMatcher(Attach.Field.INITIAL_DELIVERY_COUNT, m);
        return this;
    }

    public AttachExpectation withMaxMessageSize(Matcher<?> m) {
        matcher.addFieldMatcher(Attach.Field.MAX_MESSAGE_SIZE, m);
        return this;
    }

    public AttachExpectation withOfferedCapabilities(Matcher<?> m) {
        matcher.addFieldMatcher(Attach.Field.OFFERED_CAPABILITIES, m);
        return this;
    }

    public AttachExpectation withDesiredCapabilities(Matcher<?> m) {
        matcher.addFieldMatcher(Attach.Field.DESIRED_CAPABILITIES, m);
        return this;
    }

    public AttachExpectation withProperties(Matcher<?> m) {
        matcher.addFieldMatcher(Attach.Field.PROPERTIES, m);
        return this;
    }

    @Override
    protected Matcher<ListDescribedType> getExpectationMatcher() {
        return matcher;
    }

    @Override
    protected Object getFieldValue(Attach attach, Enum<?> performativeField) {
        return attach.getFieldValue(performativeField.ordinal());
    }

    @Override
    protected Enum<?> getFieldEnum(int fieldIndex) {
        return Attach.Field.values()[fieldIndex];
    }

    @Override
    protected Class<Attach> getExpectedTypeClass() {
        return Attach.class;
    }

    //----- Extend the Source and Target Matchers to extend the API

    public static class AttachSourceMatcher extends SourceMatcher {

        private final AttachExpectation expectation;

        public AttachSourceMatcher(AttachExpectation expectation) {
            this.expectation = expectation;
        }

        public AttachExpectation also() {
            return expectation;
        }

        public AttachExpectation and() {
            return expectation;
        }

        @Override
        public AttachSourceMatcher withAddress(String name) {
            super.withAddress(name);
            return this;
        }

        @Override
        public AttachSourceMatcher withDurable(TerminusDurability durability) {
            super.withDurable(durability);
            return this;
        }

        @Override
        public AttachSourceMatcher withExpiryPolicy(TerminusExpiryPolicy expiry) {
            super.withExpiryPolicy(expiry);
            return this;
        }

        @Override
        public AttachSourceMatcher withTimeout(int timeout) {
            super.withTimeout(timeout);
            return this;
        }

        @Override
        public AttachSourceMatcher withTimeout(long timeout) {
            super.withTimeout(timeout);
            return this;
        }

        @Override
        public AttachSourceMatcher withTimeout(UnsignedInteger timeout) {
            super.withTimeout(timeout);
            return this;
        }

        @Override
        public AttachSourceMatcher withDynamic(boolean dynamic) {
            super.withDynamic(dynamic);
            return this;
        }

        @Override
        public AttachSourceMatcher withDynamicNodeProperties(Map<Symbol, Object> properties) {
            super.withDynamicNodeProperties(properties);
            return this;
        }

        @Override
        public AttachSourceMatcher withDistributionMode(Symbol distributionMode) {
            super.withDistributionMode(distributionMode);
            return this;
        }

        @Override
        public AttachSourceMatcher withFilter(Map<Symbol, Object> filter) {
            super.withFilter(filter);
            return this;
        }

        @Override
        public AttachSourceMatcher withDefaultOutcome(DeliveryState defaultOutcome) {
            super.withDefaultOutcome(defaultOutcome);
            return this;
        }

        @Override
        public AttachSourceMatcher withOutcomes(Symbol... outcomes) {
            super.withOutcomes(outcomes);
            return this;
        }

        @Override
        public AttachSourceMatcher withCapabilities(Symbol... capabilities) {
            super.withCapabilities(capabilities);
            return this;
        }

        @Override
        public AttachSourceMatcher withAddress(Matcher<?> m) {
            super.withAddress(m);
            return this;
        }

        @Override
        public AttachSourceMatcher withDurable(Matcher<?> m) {
            super.withDurable(m);
            return this;
        }

        @Override
        public AttachSourceMatcher withExpiryPolicy(Matcher<?> m) {
            super.withExpiryPolicy(m);
            return this;
        }

        @Override
        public AttachSourceMatcher withTimeout(Matcher<?> m) {
            super.withTimeout(m);
            return this;
        }

        @Override
        public AttachSourceMatcher withDynamic(Matcher<?> m) {
            super.withDynamic(m);
            return this;
        }

        @Override
        public AttachSourceMatcher withDynamicNodeProperties(Matcher<?> m) {
            super.withDynamicNodeProperties(m);
            return this;
        }

        @Override
        public AttachSourceMatcher withDistributionMode(Matcher<?> m) {
            super.withDistributionMode(m);
            return this;
        }

        @Override
        public AttachSourceMatcher withFilter(Matcher<?> m) {
            super.withFilter(m);
            return this;
        }

        @Override
        public AttachSourceMatcher withDefaultOutcome(Matcher<?> m) {
            super.withDefaultOutcome(m);
            return this;
        }

        @Override
        public AttachSourceMatcher withOutcomes(Matcher<?> m) {
            super.withOutcomes(m);
            return this;
        }

        @Override
        public AttachSourceMatcher withCapabilities(Matcher<?> m) {
            super.withCapabilities(m);
            return this;
        }
    }

    public static class AttachTargetMatcher extends TargetMatcher {

        private final AttachExpectation expectation;

        public AttachTargetMatcher(AttachExpectation expectation) {
            this.expectation = expectation;
        }

        public AttachExpectation also() {
            return expectation;
        }

        public AttachExpectation and() {
            return expectation;
        }

        @Override
        public AttachTargetMatcher withAddress(String name) {
            super.withAddress(name);
            return this;
        }

        @Override
        public AttachTargetMatcher withDurable(TerminusDurability durability) {
            super.withDurable(durability);
            return this;
        }

        @Override
        public AttachTargetMatcher withExpiryPolicy(TerminusExpiryPolicy expiry) {
            super.withExpiryPolicy(expiry);
            return this;
        }

        @Override
        public AttachTargetMatcher withTimeout(int timeout) {
            super.withTimeout(timeout);
            return this;
        }

        @Override
        public AttachTargetMatcher withTimeout(long timeout) {
            super.withTimeout(timeout);
            return this;
        }

        @Override
        public AttachTargetMatcher withTimeout(UnsignedInteger timeout) {
            super.withTimeout(timeout);
            return this;
        }

        @Override
        public AttachTargetMatcher withDynamic(boolean dynamic) {
            super.withDynamic(dynamic);
            return this;
        }

        @Override
        public AttachTargetMatcher withDynamicNodeProperties(Map<Symbol, Object> properties) {
            super.withDynamicNodeProperties(properties);
            return this;
        }

        @Override
        public AttachTargetMatcher withCapabilities(Symbol... capabilities) {
            super.withCapabilities(capabilities);
            return this;
        }

        @Override
        public AttachTargetMatcher withAddress(Matcher<?> m) {
            super.withAddress(m);
            return this;
        }

        @Override
        public AttachTargetMatcher withDurable(Matcher<?> m) {
            super.withDurable(m);
            return this;
        }

        @Override
        public AttachTargetMatcher withExpiryPolicy(Matcher<?> m) {
            super.withExpiryPolicy(m);
            return this;
        }

        @Override
        public AttachTargetMatcher withTimeout(Matcher<?> m) {
            super.withTimeout(m);
            return this;
        }

        @Override
        public AttachTargetMatcher withDynamic(Matcher<?> m) {
            super.withDynamic(m);
            return this;
        }

        @Override
        public AttachTargetMatcher withDynamicNodeProperties(Matcher<?> m) {
            super.withDynamicNodeProperties(m);
            return this;
        }

        @Override
        public AttachTargetMatcher withCapabilities(Matcher<?> m) {
            super.withCapabilities(m);
            return this;
        }
    }
}

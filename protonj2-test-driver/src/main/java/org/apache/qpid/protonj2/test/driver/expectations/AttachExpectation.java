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
package org.apache.qpid.protonj2.test.driver.expectations;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;

import java.util.Map;
import java.util.UUID;

import org.apache.qpid.protonj2.test.driver.AMQPTestDriver;
import org.apache.qpid.protonj2.test.driver.LinkTracker;
import org.apache.qpid.protonj2.test.driver.SessionTracker;
import org.apache.qpid.protonj2.test.driver.actions.AttachInjectAction;
import org.apache.qpid.protonj2.test.driver.actions.BeginInjectAction;
import org.apache.qpid.protonj2.test.driver.actions.DetachInjectAction;
import org.apache.qpid.protonj2.test.driver.codec.ListDescribedType;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Source;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Target;
import org.apache.qpid.protonj2.test.driver.codec.messaging.TerminusDurability;
import org.apache.qpid.protonj2.test.driver.codec.messaging.TerminusExpiryPolicy;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Binary;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Symbol;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedByte;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedInteger;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedLong;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedShort;
import org.apache.qpid.protonj2.test.driver.codec.transactions.Coordinator;
import org.apache.qpid.protonj2.test.driver.codec.transport.Attach;
import org.apache.qpid.protonj2.test.driver.codec.transport.DeliveryState;
import org.apache.qpid.protonj2.test.driver.codec.transport.ReceiverSettleMode;
import org.apache.qpid.protonj2.test.driver.codec.transport.Role;
import org.apache.qpid.protonj2.test.driver.codec.transport.SenderSettleMode;
import org.apache.qpid.protonj2.test.driver.codec.util.TypeMapper;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.SourceMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.TargetMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.transactions.CoordinatorMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.transport.AttachMatcher;
import org.hamcrest.Matcher;

import io.netty.buffer.ByteBuf;

/**
 * Scripted expectation for the AMQP Attach performative
 */
public class AttachExpectation extends AbstractExpectation<Attach> {

    private final AttachMatcher matcher = new AttachMatcher();

    private AttachInjectAction response;
    private boolean rejecting;

    public AttachExpectation(AMQPTestDriver driver) {
        super(driver);

        // Configure default expectations for a valid Attach
        withName(notNullValue());
        withHandle(notNullValue());
        withRole(notNullValue());
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

    public DetachInjectAction reject(boolean close, String condition, String description) {
        return reject(close, Symbol.valueOf(condition), description);
    }

    public DetachInjectAction reject(boolean close, Symbol condition, String description) {
        rejecting = true;
        response = new AttachInjectAction(driver);
        driver.addScriptedElement(response);

        DetachInjectAction action =
            new DetachInjectAction(driver).withClosed(close).withErrorCondition(condition, description);
        driver.addScriptedElement(action);

        return action;
    }

    //----- Handle the performative and configure response is told to respond

    @Override
    public void handleAttach(Attach attach, ByteBuf payload, int channel, AMQPTestDriver context) {
        super.handleAttach(attach, payload, channel, context);

        final UnsignedShort remoteChannel = UnsignedShort.valueOf(channel);
        final SessionTracker session = driver.sessions().getSessionFromRemoteChannel(remoteChannel);

        if (session == null) {
            throw new AssertionError(String.format(
                "Received Attach on channel [%d] that has no matching Session for that remote channel. ", remoteChannel));
        }

        final LinkTracker link = session.handleRemoteAttach(attach);

        if (response != null) {
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
            if (response.getPerformative().getSenderSettleMode() == null) {
                response.withSndSettleMode(SenderSettleMode.valueOf(attach.getSenderSettleMode()));
            }
            if (response.getPerformative().getReceiverSettleMode() == null) {
                response.withRcvSettleMode(ReceiverSettleMode.valueOf(attach.getReceiverSettleMode()));
            }
            if (response.getPerformative().getSource() == null && !response.isNullSourceRequired()) {
                response.withSource(attach.getSource());
                if (attach.getSource() != null && Boolean.TRUE.equals(attach.getSource().getDynamic())) {
                    attach.getSource().setAddress(UUID.randomUUID().toString());
                }
            }

            if (rejecting) {
                if (Boolean.FALSE.equals(attach.getRole())) {
                    // Sender attach so response should have null target
                    response.withNullTarget();
                } else {
                    // Receiver attach so response should have null source
                    response.withNullSource();
                }
            }

            if (response.getPerformative().getTarget() == null && !response.isNullTargetRequired()) {
                if (attach.getTarget() != null) {
                    if (attach.getTarget() instanceof org.apache.qpid.protonj2.test.driver.codec.messaging.Target) {
                        org.apache.qpid.protonj2.test.driver.codec.messaging.Target target =
                            (org.apache.qpid.protonj2.test.driver.codec.messaging.Target) attach.getTarget();
                        response.withTarget(target);
                        if (target != null && Boolean.TRUE.equals(target.getDynamic())) {
                            target.setAddress(UUID.randomUUID().toString());
                        }
                    } else {
                        org.apache.qpid.protonj2.test.driver.codec.transactions.Coordinator coordinator =
                            (org.apache.qpid.protonj2.test.driver.codec.transactions.Coordinator) attach.getTarget();
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

    public AttachExpectation withRole(boolean role) {
        return withRole(equalTo(role));
    }

    public AttachExpectation withRole(Boolean role) {
        return withRole(equalTo(role));
    }

    public AttachExpectation withRole(Role role) {
        return withRole(equalTo(role.getValue()));
    }

    public AttachExpectation ofSender() {
        return withRole(equalTo(Role.SENDER.getValue()));
    }

    public AttachExpectation ofReceiver() {
        return withRole(equalTo(Role.RECEIVER.getValue()));
    }

    public AttachExpectation withSndSettleMode(byte sndSettleMode) {
        return withSndSettleMode(equalTo(UnsignedByte.valueOf(sndSettleMode)));
    }

    public AttachExpectation withSndSettleMode(Byte sndSettleMode) {
        return withSndSettleMode(sndSettleMode == null ? nullValue() : equalTo(UnsignedByte.valueOf(sndSettleMode.byteValue())));
    }

    public AttachExpectation withSndSettleMode(SenderSettleMode sndSettleMode) {
        return withSndSettleMode(sndSettleMode == null ? nullValue() : equalTo(sndSettleMode.getValue()));
    }

    public AttachExpectation withSenderSettleModeMixed() {
        return withSndSettleMode(equalTo(SenderSettleMode.MIXED.getValue()));
    }

    public AttachExpectation withSenderSettleModeSettled() {
        return withSndSettleMode(equalTo(SenderSettleMode.SETTLED.getValue()));
    }

    public AttachExpectation withSenderSettleModeUnsettled() {
        return withSndSettleMode(equalTo(SenderSettleMode.UNSETTLED.getValue()));
    }

    public AttachExpectation withRcvSettleMode(byte rcvSettleMode) {
        return withRcvSettleMode(equalTo(UnsignedByte.valueOf(rcvSettleMode)));
    }

    public AttachExpectation withRcvSettleMode(Byte rcvSettleMode) {
        return withRcvSettleMode(rcvSettleMode == null ? nullValue() : equalTo(UnsignedByte.valueOf(rcvSettleMode.byteValue())));
    }

    public AttachExpectation withRcvSettleMode(ReceiverSettleMode rcvSettleMode) {
        return withRcvSettleMode(rcvSettleMode == null ? nullValue() : equalTo(rcvSettleMode.getValue()));
    }

    public AttachExpectation withReceivervSettlesFirst() {
        return withRcvSettleMode(equalTo(ReceiverSettleMode.FIRST.getValue()));
    }

    public AttachExpectation withReceivervSettlesSecond() {
        return withRcvSettleMode(equalTo(ReceiverSettleMode.SECOND.getValue()));
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

    public AttachCoordinatorMatcher withCoordinator() {
        AttachCoordinatorMatcher matcher = new AttachCoordinatorMatcher(this);
        withCoordinator(matcher);
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

    public AttachExpectation withCoordinator(Coordinator coordinator) {
        if (coordinator != null) {
            CoordinatorMatcher coordinatorMatcher = new CoordinatorMatcher();
            return withCoordinator(coordinatorMatcher);
        } else {
            return withCoordinator(nullValue());
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

    public AttachExpectation withOfferedCapabilities(String... offeredCapabilities) {
        return withOfferedCapabilities(equalTo(TypeMapper.toSymbolArray(offeredCapabilities)));
    }

    public AttachExpectation withDesiredCapabilities(Symbol... desiredCapabilities) {
        return withDesiredCapabilities(equalTo(desiredCapabilities));
    }

    public AttachExpectation withDesiredCapabilities(String... desiredCapabilities) {
        return withDesiredCapabilities(equalTo(TypeMapper.toSymbolArray(desiredCapabilities)));
    }

    public AttachExpectation withPropertiesMap(Map<Symbol, Object> properties) {
        return withProperties(equalTo(properties));
    }

    public AttachExpectation withProperties(Map<String, Object> properties) {
        return withProperties(equalTo(TypeMapper.toSymbolKeyedMap(properties)));
    }

    //----- Matcher based with methods for more complex validation

    public AttachExpectation withName(Matcher<?> m) {
        matcher.withName(m);
        return this;
    }

    public AttachExpectation withHandle(Matcher<?> m) {
        matcher.withHandle(m);
        return this;
    }

    public AttachExpectation withRole(Matcher<?> m) {
        matcher.withRole(m);
        return this;
    }

    public AttachExpectation withSndSettleMode(Matcher<?> m) {
        matcher.withSndSettleMode(m);
        return this;
    }

    public AttachExpectation withRcvSettleMode(Matcher<?> m) {
        matcher.withRcvSettleMode(m);
        return this;
    }

    public AttachExpectation withSource(Matcher<?> m) {
        matcher.withSource(m);
        return this;
    }

    public AttachExpectation withTarget(Matcher<?> m) {
        matcher.withTarget(m);
        return this;
    }

    public AttachExpectation withCoordinator(Matcher<?> m) {
        matcher.withCoordinator(m);
        return this;
    }

    public AttachExpectation withUnsettled(Matcher<?> m) {
        matcher.withUnsettled(m);
        return this;
    }

    public AttachExpectation withIncompleteUnsettled(Matcher<?> m) {
        matcher.withIncompleteUnsettled(m);
        return this;
    }

    public AttachExpectation withInitialDeliveryCount(Matcher<?> m) {
        matcher.withInitialDeliveryCount(m);
        return this;
    }

    public AttachExpectation withMaxMessageSize(Matcher<?> m) {
        matcher.withMaxMessageSize(m);
        return this;
    }

    public AttachExpectation withOfferedCapabilities(Matcher<?> m) {
        matcher.withOfferedCapabilities(m);
        return this;
    }

    public AttachExpectation withDesiredCapabilities(Matcher<?> m) {
        matcher.withDesiredCapabilities(m);
        return this;
    }

    public AttachExpectation withProperties(Matcher<?> m) {
        matcher.withProperties(m);
        return this;
    }

    @Override
    protected Matcher<ListDescribedType> getExpectationMatcher() {
        return matcher;
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
        public AttachSourceMatcher withDynamicNodePropertiesMap(Map<Symbol, Object> properties) {
            super.withDynamicNodePropertiesMap(properties);
            return this;
        }

        @Override
        public AttachSourceMatcher withDynamicNodeProperties(Map<String, Object> properties) {
            super.withDynamicNodeProperties(properties);
            return this;
        }

        @Override
        public AttachSourceMatcher withDistributionMode(String distributionMode) {
            super.withDistributionMode(distributionMode);
            return this;
        }

        @Override
        public AttachSourceMatcher withDistributionMode(Symbol distributionMode) {
            super.withDistributionMode(distributionMode);
            return this;
        }

        @Override
        public AttachSourceMatcher withFilter(Map<String, Object> filter) {
            super.withFilter(filter);
            return this;
        }

        @Override
        public AttachSourceMatcher withDefaultOutcome(DeliveryState defaultOutcome) {
            super.withDefaultOutcome(defaultOutcome);
            return this;
        }

        @Override
        public AttachSourceMatcher withOutcomes(String... outcomes) {
            super.withOutcomes(outcomes);
            return this;
        }

        @Override
        public AttachSourceMatcher withOutcomes(Symbol... outcomes) {
            super.withOutcomes(outcomes);
            return this;
        }

        @Override
        public AttachSourceMatcher withCapabilities(String... capabilities) {
            super.withCapabilities(capabilities);
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
        public AttachSourceMatcher withDefaultTimeout() {
            super.withTimeout(anyOf(nullValue(), equalTo(UnsignedInteger.ZERO)));
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
        public AttachTargetMatcher withDefaultTimeout() {
            super.withDefaultTimeout();
            return this;
        }

        @Override
        public AttachTargetMatcher withDynamic(boolean dynamic) {
            super.withDynamic(dynamic);
            return this;
        }

        @Override
        public AttachTargetMatcher withDynamicNodeProperties(Map<String, Object> properties) {
            super.withDynamicNodeProperties(properties);
            return this;
        }

        @Override
        public AttachTargetMatcher withCapabilities(Symbol... capabilities) {
            super.withCapabilities(capabilities);
            return this;
        }

        @Override
        public AttachTargetMatcher withCapabilities(String... capabilities) {
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

    public static class AttachCoordinatorMatcher extends CoordinatorMatcher {

        private final AttachExpectation expectation;

        public AttachCoordinatorMatcher(AttachExpectation expectation) {
            this.expectation = expectation;
        }

        public AttachExpectation also() {
            return expectation;
        }

        public AttachExpectation and() {
            return expectation;
        }

        @Override
        public AttachCoordinatorMatcher withCapabilities(Symbol... capabilities) {
            super.withCapabilities(capabilities);
            return this;
        }

        @Override
        public AttachCoordinatorMatcher withCapabilities(String... capabilities) {
            super.withCapabilities(capabilities);
            return this;
        }
    }
}

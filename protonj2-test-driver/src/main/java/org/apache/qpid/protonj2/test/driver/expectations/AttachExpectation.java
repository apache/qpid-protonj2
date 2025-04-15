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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Predicate;

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
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedInteger;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedLong;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedShort;
import org.apache.qpid.protonj2.test.driver.codec.transactions.Coordinator;
import org.apache.qpid.protonj2.test.driver.codec.transport.Attach;
import org.apache.qpid.protonj2.test.driver.codec.transport.DeliveryState;
import org.apache.qpid.protonj2.test.driver.codec.transport.ReceiverSettleMode;
import org.apache.qpid.protonj2.test.driver.codec.transport.Role;
import org.apache.qpid.protonj2.test.driver.codec.transport.SenderSettleMode;
import org.apache.qpid.protonj2.test.driver.matchers.JmsNoLocalByIdDescribedType;
import org.apache.qpid.protonj2.test.driver.matchers.JmsSelectorByIdDescribedType;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.SourceMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.TargetMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.transactions.CoordinatorMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.transport.AttachMatcher;
import org.hamcrest.Matcher;

/**
 * Scripted expectation for the AMQP Attach performative
 */
public class AttachExpectation extends AbstractExpectation<Attach> {

    private final AttachMatcher matcher = new AttachMatcher();

    private AttachInjectAction response;
    private boolean rejecting;
    private boolean inKindResponse;

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

    @Override
    public AttachExpectation optional() {
        return (AttachExpectation) super.optional();
    }

    @Override
    public AttachExpectation withPredicate(Predicate<Attach> predicate) {
        super.withPredicate(predicate);
        return this;
    }

    @Override
    public AttachExpectation withCapture(Consumer<Attach> capture) {
        super.withCapture(capture);
        return this;
    }

    /**
     * Creates a sufficient response for a simple {@link Attach} request for
     * simple test scripts. This response does not offer capabilities to the
     * remote which means that the scripting code may need to add any that it
     * wants to or validate errors on the remote if desired capabilities are
     * absent.
     *
     * @return the {@link Attach} injection action that will be used to respond.
     */
    public AttachInjectAction respond() {
        response = new AttachInjectAction(driver);
        driver.addScriptedElement(response);
        return response;
    }

    /**
     * More complete auto response than normal where the response attempts to match
     * all requested capabilities etc in the Attach response so that the script writer
     * can assume that the response to the attach request is a valid and complete
     * response without need to complete the offered capabilities in response to the
     * remote's desired capabilities etc.
     * <p>
     * Use this with a bit of care as it will overwrite any script defined response
     * values in favor of an in-kind version.
     *
     * @return the {@link Attach} injection action that will be used to respond.
     */
    public AttachInjectAction respondInKind() {
        response = new AttachInjectAction(driver);
        inKindResponse = true;
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
    public void handleAttach(int frameSize, Attach attach, ByteBuffer payload, int channel, AMQPTestDriver context) {
        super.handleAttach(frameSize, attach, payload, channel, context);

        final UnsignedShort remoteChannel = UnsignedShort.valueOf(channel);
        final SessionTracker session = driver.sessions().getSessionFromRemoteChannel(remoteChannel);

        if (session == null) {
            throw new AssertionError(String.format(
                "Received Attach on channel [%s] that has no matching Session for that remote channel. ", remoteChannel));
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
                response.withHandle(session.findFreeLocalHandle());
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
                    if (attach.getTarget() instanceof Target) {
                        Target target = (Target) attach.getTarget();
                        response.withTarget(target);
                        if (target != null && Boolean.TRUE.equals(target.getDynamic())) {
                            target.setAddress(UUID.randomUUID().toString());
                        }
                    } else {
                        Coordinator coordinator = (Coordinator) attach.getTarget();
                        response.withTarget(coordinator);
                    }
                }
            }


            if (response.getPerformative().getInitialDeliveryCount() == null && !response.isExplicitlyNullDeliveryCount()) {
                Role role = Role.valueOf(response.getPerformative().getRole());
                if (role == Role.SENDER) {
                    response.withInitialDeliveryCount(0);
                }
            }

            if (inKindResponse) {
                final Symbol[] desired = attach.getDesiredCapabilities();
                if (desired != null && desired.length > 0) {
                    response.withOfferedCapabilities(Arrays.copyOf(desired, desired.length));
                }

                if (attach.getMaxMessageSize() != null) {
                    response.withMaxMessageSize(attach.getMaxMessageSize());
                }
            }

            // Other fields are left not set for now unless test script configured
        }
    }

    //----- Type specific with methods that perform simple equals checks

    public AttachExpectation withName(String name) {
        matcher.withName(name);
        return this;
    }

    public AttachExpectation withHandle(int handle) {
        matcher.withHandle(handle);
        return this;
    }

    public AttachExpectation withHandle(long handle) {
        matcher.withHandle(handle);
        return this;
    }

    public AttachExpectation withHandle(UnsignedInteger handle) {
        matcher.withHandle(handle);
        return this;
    }

    public AttachExpectation withRole(boolean role) {
        matcher.withRole(role);
        return this;
    }

    public AttachExpectation withRole(Boolean role) {
        matcher.withRole(role);
        return this;
    }

    public AttachExpectation withRole(Role role) {
        matcher.withRole(role);
        return this;
    }

    public AttachExpectation ofSender() {
        return withRole(Role.SENDER);
    }

    public AttachExpectation ofReceiver() {
        return withRole(Role.RECEIVER);
    }

    public AttachExpectation withSndSettleMode(byte sndSettleMode) {
        matcher.withSndSettleMode(sndSettleMode);
        return this;
    }

    public AttachExpectation withSndSettleMode(Byte sndSettleMode) {
        matcher.withSndSettleMode(sndSettleMode);
        return this;
    }

    public AttachExpectation withSndSettleMode(SenderSettleMode sndSettleMode) {
        matcher.withSndSettleMode(sndSettleMode);
        return this;
    }

    public AttachExpectation withSenderSettleModeMixed() {
        return withSndSettleMode(SenderSettleMode.MIXED);
    }

    public AttachExpectation withSenderSettleModeSettled() {
        return withSndSettleMode(SenderSettleMode.SETTLED);
    }

    public AttachExpectation withSenderSettleModeUnsettled() {
        return withSndSettleMode(SenderSettleMode.UNSETTLED);
    }

    public AttachExpectation withRcvSettleMode(byte rcvSettleMode) {
        matcher.withRcvSettleMode(rcvSettleMode);
        return this;
    }

    public AttachExpectation withRcvSettleMode(Byte rcvSettleMode) {
        matcher.withRcvSettleMode(rcvSettleMode);
        return this;
    }

    public AttachExpectation withRcvSettleMode(ReceiverSettleMode rcvSettleMode) {
        matcher.withRcvSettleMode(rcvSettleMode);
        return this;
    }

    public AttachExpectation withReceiverSettlesFirst() {
        return withRcvSettleMode(ReceiverSettleMode.FIRST);
    }

    public AttachExpectation withReceiverSettlesSecond() {
        return withRcvSettleMode(ReceiverSettleMode.SECOND);
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

    public AttachExpectation withNullSource() {
        return withSource(nullValue());
    }

    public AttachExpectation withSource(Source source) {
        if (source != null) {
            SourceMatcher sourceMatcher = new SourceMatcher(source);
            return withSource(sourceMatcher);
        } else {
            return withSource(nullValue());
        }
    }

    public AttachExpectation withNullTarget() {
        return withTarget(nullValue());
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
        matcher.withUnsettled(unsettled);
        return this;
    }

    public AttachExpectation withIncompleteUnsettled(boolean incomplete) {
        matcher.withIncompleteUnsettled(incomplete);
        return this;
    }

    public AttachExpectation withInitialDeliveryCount(int initialDeliveryCount) {
        matcher.withInitialDeliveryCount(initialDeliveryCount);
        return this;
    }

    public AttachExpectation withInitialDeliveryCount(long initialDeliveryCount) {
        matcher.withInitialDeliveryCount(initialDeliveryCount);
        return this;
    }

    public AttachExpectation withInitialDeliveryCount(UnsignedInteger initialDeliveryCount) {
        matcher.withInitialDeliveryCount(initialDeliveryCount);
        return this;
    }

    public AttachExpectation withMaxMessageSize(long maxMessageSize) {
        matcher.withMaxMessageSize(maxMessageSize);
        return this;
    }

    public AttachExpectation withMaxMessageSize(UnsignedLong maxMessageSize) {
        matcher.withMaxMessageSize(maxMessageSize);
        return this;
    }

    public AttachExpectation withOfferedCapabilities(Symbol... offeredCapabilities) {
        matcher.withOfferedCapabilities(offeredCapabilities);
        return this;
    }

    public AttachExpectation withOfferedCapabilities(String... offeredCapabilities) {
        matcher.withOfferedCapabilities(offeredCapabilities);
        return this;
    }

    public AttachExpectation withDesiredCapabilities(Symbol... desiredCapabilities) {
        matcher.withDesiredCapabilities(desiredCapabilities);
        return this;
    }

    public AttachExpectation withDesiredCapabilities(String... desiredCapabilities) {
        matcher.withDesiredCapabilities(desiredCapabilities);
        return this;
    }

    public AttachExpectation withPropertiesMap(Map<Symbol, Object> properties) {
        matcher.withPropertiesMap(properties);
        return this;
    }

    public AttachExpectation withProperties(Map<String, Object> properties) {
        matcher.withProperties(properties);
        return this;
    }

    public AttachExpectation withProperty(String key, Object value) {
        matcher.withProperty(key, value);
        return this;
    }

    public AttachExpectation withProperty(Symbol key, Object value) {
        matcher.withProperty(key, value);
        return this;
    }

    public AttachExpectation withDesiredCapability(Symbol desiredCapability) {
        matcher.withDesiredCapability(desiredCapability);
        return this;
    }

    public AttachExpectation withDesiredCapability(String desiredCapability) {
        matcher.withDesiredCapability(desiredCapability);
        return this;
    }

    public AttachExpectation withOfferedCapability(Symbol offeredCapability) {
        matcher.withOfferedCapability(offeredCapability);
        return this;
    }

    public AttachExpectation withOfferedCapability(String offeredCapability) {
        matcher.withOfferedCapability(offeredCapability);
        return this;
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

        public AttachSourceMatcher withJMSSelector(String selector) {
            final JmsSelectorByIdDescribedType filterType = new JmsSelectorByIdDescribedType(selector);
            final Map<String, Object> filtersMap = new HashMap<>();

            filtersMap.put(JmsSelectorByIdDescribedType.JMS_SELECTOR_KEY, filterType);

            super.withFilter(filtersMap);
            return this;
        }

        public AttachSourceMatcher withNoLocal() {
            final JmsNoLocalByIdDescribedType filterType = new JmsNoLocalByIdDescribedType();
            final Map<String, Object> filtersMap = new HashMap<>();

            filtersMap.put(JmsNoLocalByIdDescribedType.JMS_NO_LOCAL_KEY, filterType);

            super.withFilter(filtersMap);
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

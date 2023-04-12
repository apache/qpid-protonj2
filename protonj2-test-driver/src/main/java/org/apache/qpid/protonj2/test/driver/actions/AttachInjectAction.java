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
package org.apache.qpid.protonj2.test.driver.actions;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.qpid.protonj2.test.driver.AMQPTestDriver;
import org.apache.qpid.protonj2.test.driver.SessionTracker;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Outcome;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Source;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Target;
import org.apache.qpid.protonj2.test.driver.codec.messaging.TerminusDurability;
import org.apache.qpid.protonj2.test.driver.codec.messaging.TerminusExpiryPolicy;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Binary;
import org.apache.qpid.protonj2.test.driver.codec.primitives.DescribedType;
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

/**
 * AMQP Attach injection action which can be added to a driver for write at a specific time or
 * following on from some other action in the test script.
 */
public class AttachInjectAction extends AbstractPerformativeInjectAction<Attach> {

    private final Attach attach = new Attach();

    private boolean explicitlyNullName;
    private boolean explicitlyNullHandle;
    private boolean nullSourceRequired;
    private boolean nullTargetRequired;

    public AttachInjectAction(AMQPTestDriver driver) {
        super(driver);
    }

    @Override
    public Attach getPerformative() {
        return attach;
    }

    public AttachInjectAction withName(String name) {
        explicitlyNullName = name == null;
        attach.setName(name);
        return this;
    }

    public AttachInjectAction withHandle(int handle) {
        attach.setHandle(UnsignedInteger.valueOf(handle));
        return this;
    }

    public AttachInjectAction withHandle(long handle) {
        attach.setHandle(UnsignedInteger.valueOf(handle));
        return this;
    }

    public AttachInjectAction withHandle(UnsignedInteger handle) {
        explicitlyNullHandle = handle == null;
        attach.setHandle(handle);
        return this;
    }

    public AttachInjectAction withRole(boolean role) {
        attach.setRole(role);
        return this;
    }

    public AttachInjectAction withRole(Role role) {
        attach.setRole(role.getValue());
        return this;
    }

    public AttachInjectAction ofSender() {
        attach.setRole(Role.SENDER.getValue());
        return this;
    }

    public AttachInjectAction ofReceiver() {
        attach.setRole(Role.RECEIVER.getValue());
        return this;
    }

    public AttachInjectAction withSndSettleMode(byte sndSettleMode) {
        attach.setSenderSettleMode(UnsignedByte.valueOf(sndSettleMode));
        return this;
    }

    public AttachInjectAction withSndSettleMode(Byte sndSettleMode) {
        attach.setSenderSettleMode(sndSettleMode == null ? null : UnsignedByte.valueOf(sndSettleMode.byteValue()));
        return this;
    }

    public AttachInjectAction withSndSettleMode(SenderSettleMode sndSettleMode) {
        attach.setSenderSettleMode(sndSettleMode == null ? null : sndSettleMode.getValue());
        return this;
    }

    public AttachInjectAction withSenderSettleModeMixed() {
        attach.setSenderSettleMode(SenderSettleMode.MIXED.getValue());
        return this;
    }

    public AttachInjectAction withSenderSettleModeSettled() {
        attach.setSenderSettleMode(SenderSettleMode.SETTLED.getValue());
        return this;
    }

    public AttachInjectAction withSenderSettleModeUnsettled() {
        attach.setSenderSettleMode(SenderSettleMode.UNSETTLED.getValue());
        return this;
    }

    public AttachInjectAction withRcvSettleMode(byte rcvSettleMode) {
        attach.setReceiverSettleMode(UnsignedByte.valueOf(rcvSettleMode));
        return this;
    }

    public AttachInjectAction withRcvSettleMode(Byte rcvSettleMode) {
        attach.setReceiverSettleMode(rcvSettleMode == null ? null : UnsignedByte.valueOf(rcvSettleMode.byteValue()));
        return this;
    }

    public AttachInjectAction withRcvSettleMode(ReceiverSettleMode rcvSettleMode) {
        attach.setReceiverSettleMode(rcvSettleMode == null ? null : rcvSettleMode.getValue());
        return this;
    }

    public AttachInjectAction withReceivervSettlesFirst() {
        attach.setReceiverSettleMode(ReceiverSettleMode.FIRST.getValue());
        return this;
    }

    public AttachInjectAction withReceivervSettlesSecond() {
        attach.setReceiverSettleMode(ReceiverSettleMode.SECOND.getValue());
        return this;
    }

    public boolean isNullSourceRequired() {
        return nullSourceRequired;
    }

    public AttachInjectAction withNullSource() {
        nullSourceRequired = true;
        attach.setSource(null);
        return this;
    }

    public SourceBuilder withSource() {
        nullSourceRequired = false;
        return new SourceBuilder(getOrCreateSource());
    }

    public AttachInjectAction withSource(Source source) {
        nullSourceRequired = source == null;
        attach.setSource(source);
        return this;
    }

    public boolean isNullTargetRequired() {
        return nullTargetRequired;
    }

    public AttachInjectAction withNullTarget() {
        nullTargetRequired = true;
        attach.setTarget((Target) null);
        return this;
    }

    public TargetBuilder withTarget() {
        nullSourceRequired = false;
        return new TargetBuilder(getOrCreateTarget());
    }

    public CoordinatorBuilder withCoordinator() {
        nullSourceRequired = false;
        return new CoordinatorBuilder(getOrCreateCoordinator());
    }

    public AttachInjectAction withTarget(Target target) {
        nullTargetRequired = target == null;
        attach.setTarget(target);
        return this;
    }

    public AttachInjectAction withTarget(Coordinator coordinator) {
        nullTargetRequired = coordinator == null;
        attach.setTarget(coordinator);
        return this;
    }

    public AttachInjectAction withUnsettled(Map<Binary, DeliveryState> unsettled) {
        if (unsettled != null) {
            Map<Binary, DescribedType> converted = new LinkedHashMap<>();
            for (Entry<Binary, DeliveryState> entry : unsettled.entrySet()) {
                converted.put(entry.getKey(), entry.getValue());
            }

            attach.setUnsettled(converted);
        }
        return this;
    }

    public AttachInjectAction withIncompleteUnsettled(boolean incomplete) {
        attach.setIncompleteUnsettled(incomplete);
        return this;
    }

    public AttachInjectAction withInitialDeliveryCount(long initialDeliveryCount) {
        attach.setInitialDeliveryCount(UnsignedInteger.valueOf(initialDeliveryCount));
        return this;
    }

    public AttachInjectAction withMaxMessageSize(UnsignedLong maxMessageSize) {
        attach.setMaxMessageSize(maxMessageSize);
        return this;
    }

    public AttachInjectAction withOfferedCapabilities(String... offeredCapabilities) {
        attach.setOfferedCapabilities(TypeMapper.toSymbolArray(offeredCapabilities));
        return this;
    }

    public AttachInjectAction withOfferedCapabilities(Symbol... offeredCapabilities) {
        attach.setOfferedCapabilities(offeredCapabilities);
        return this;
    }

    public AttachInjectAction withDesiredCapabilities(String... desiredCapabilities) {
        attach.setDesiredCapabilities(TypeMapper.toSymbolArray(desiredCapabilities));
        return this;
    }

    public AttachInjectAction withDesiredCapabilities(Symbol... desiredCapabilities) {
        attach.setDesiredCapabilities(desiredCapabilities);
        return this;
    }

    public AttachInjectAction withPropertiesMap(Map<String, Object> properties) {
        attach.setProperties(TypeMapper.toSymbolKeyedMap(properties));
        return this;
    }

    public AttachInjectAction withProperties(Map<Symbol, Object> properties) {
        attach.setProperties(properties);
        return this;
    }

    public AttachInjectAction withProperty(Symbol key, Object value) {
        if (attach.getProperties() == null) {
            attach.setProperties(new LinkedHashMap<>());
        }

        attach.getProperties().put(key, value);
        return this;
    }

    public AttachInjectAction withProperty(String key, Object value) {
        return withProperty(Symbol.valueOf(key), value);
    }

    @Override
    protected void beforeActionPerformed(AMQPTestDriver driver) {
        // A test that is trying to send an unsolicited attach must provide a channel as we
        // won't attempt to make up one since we aren't sure what the intent here is.
        if (onChannel() == CHANNEL_UNSET) {
            if (driver.sessions().getLastLocallyOpenedSession() == null) {
                throw new AssertionError("Scripted Action cannot run without a configured channel: " +
                                         "No locally opened session exists to auto select a channel.");
            }

            onChannel(driver.sessions().getLastLocallyOpenedSession().getLocalChannel().intValue());
        }

        final UnsignedShort localChannel = UnsignedShort.valueOf(onChannel());
        final SessionTracker session = driver.sessions().getSessionFromLocalChannel(localChannel);

        // A test might be trying to send Attach outside of session scope to check for error handling
        // of unexpected performatives so we just allow no session cases and send what we are told.
        if (session != null) {
            if (attach.getName() == null && !explicitlyNullName) {
                attach.setName(UUID.randomUUID().toString());
            }

            if (attach.getHandle() == null && !explicitlyNullHandle) {
                attach.setHandle(session.findFreeLocalHandle());
            }

            // Do not signal the session that we created a link if it carries an invalid null handle
            // as that would trigger other exceptions, just pass it on as the test is likely trying
            // to validate something specific.
            if (attach.getHandle() != null) {
                session.handleLocalAttach(attach);
            }
        } else {
            if (attach.getHandle() == null && !explicitlyNullHandle) {
                throw new AssertionError("Attach must carry a handle or have an explicitly set null handle.");
            }
        }
    }

    private Source getOrCreateSource() {
        if (attach.getSource() == null) {
            attach.setSource(new Source());
        }
        return attach.getSource();
    }

    private Target getOrCreateTarget() {
        if (attach.getTarget() == null) {
            attach.setTarget(new Target());
        }
        return (Target) attach.getTarget();
    }

    private Coordinator getOrCreateCoordinator() {
        if (attach.getTarget() == null) {
            attach.setTarget(new Coordinator());
        }
        return (Coordinator) attach.getTarget();
    }

    //----- Builders for Source and Target to make test writing simpler

    protected abstract class TerminusBuilder {

        public AttachInjectAction also() {
            return AttachInjectAction.this;
        }

        public AttachInjectAction and() {
            return AttachInjectAction.this;
        }
    }

    public final class SourceBuilder extends TerminusBuilder {

        private final Source source;

        public SourceBuilder(Source source) {
            this.source = source;
        }

        public SourceBuilder withAddress(String address) {
            source.setAddress(address);
            return this;
        }

        public SourceBuilder withDurability(TerminusDurability durability) {
            source.setDurable(durability.getValue());
            return this;
        }

        public SourceBuilder withExpiryPolicy(TerminusExpiryPolicy expiryPolicy) {
            source.setExpiryPolicy(expiryPolicy.getPolicy());
            return this;
        }

        public SourceBuilder withTimeout(int timeout) {
            source.setTimeout(UnsignedInteger.valueOf(timeout));
            return this;
        }

        public SourceBuilder withTimeout(long timeout) {
            source.setTimeout(UnsignedInteger.valueOf(timeout));
            return this;
        }

        public SourceBuilder withTimeout(UnsignedInteger timeout) {
            source.setTimeout(timeout);
            return this;
        }

        public SourceBuilder withDynamic(boolean dynamic) {
            source.setDynamic(Boolean.valueOf(dynamic));
            return this;
        }

        public SourceBuilder withDynamic(Boolean dynamic) {
            source.setDynamic(dynamic);
            return this;
        }

        public SourceBuilder withDynamicNodePropertiesMap(Map<Symbol, Object> properties) {
            source.setDynamicNodeProperties(properties);
            return this;
        }

        public SourceBuilder withDynamicNodeProperties(Map<String, Object> properties) {
            source.setDynamicNodeProperties(TypeMapper.toSymbolKeyedMap(properties));
            return this;
        }

        public SourceBuilder withDistributionMode(String mode) {
            source.setDistributionMode(Symbol.valueOf(mode));
            return this;
        }

        public SourceBuilder withDistributionMode(Symbol mode) {
            source.setDistributionMode(mode);
            return this;
        }

        public SourceBuilder withFilter(Map<Symbol, Object> filters) {
            source.setFilter(filters);
            return this;
        }

        public SourceBuilder withFilterMap(Map<String, Object> filters) {
            source.setFilter(TypeMapper.toSymbolKeyedMap(filters));
            return this;
        }

        public SourceBuilder withDefaultOutcome(Outcome outcome) {
            source.setDefaultOutcome((DescribedType) outcome);
            return this;
        }

        public SourceBuilder withOutcomes(Symbol... outcomes) {
            source.setOutcomes(outcomes);
            return this;
        }

        public SourceBuilder withOutcomes(String... outcomes) {
            source.setOutcomes(outcomes);
            return this;
        }

        public SourceBuilder withCapabilities(Symbol... capabilities) {
            source.setCapabilities(capabilities);
            return this;
        }

        public SourceBuilder withCapabilities(String... capabilities) {
            source.setCapabilities(capabilities);
            return this;
        }
    }

    public final class TargetBuilder extends TerminusBuilder {

        private final Target target;

        public TargetBuilder(Target target) {
            this.target = target;
        }

        public TargetBuilder withAddress(String address) {
            target.setAddress(address);
            return this;
        }

        public TargetBuilder withDurability(TerminusDurability durability) {
            target.setDurable(durability.getValue());
            return this;
        }

        public TargetBuilder withExpiryPolicy(TerminusExpiryPolicy expiryPolicy) {
            target.setExpiryPolicy(expiryPolicy.getPolicy());
            return this;
        }

        public TargetBuilder withTimeout(int timeout) {
            target.setTimeout(UnsignedInteger.valueOf(timeout));
            return this;
        }

        public TargetBuilder withTimeout(long timeout) {
            target.setTimeout(UnsignedInteger.valueOf(timeout));
            return this;
        }

        public TargetBuilder withTimeout(UnsignedInteger timeout) {
            target.setTimeout(timeout);
            return this;
        }

        public TargetBuilder withDynamic(boolean dynamic) {
            target.setDynamic(Boolean.valueOf(dynamic));
            return this;
        }

        public TargetBuilder withDynamic(Boolean dynamic) {
            target.setDynamic(dynamic);
            return this;
        }

        public TargetBuilder withDynamicNodePropertiesMap(Map<Symbol, Object> properties) {
            target.setDynamicNodeProperties(properties);
            return this;
        }

        public TargetBuilder withDynamicNodeProperties(Map<String, Object> properties) {
            target.setDynamicNodeProperties(TypeMapper.toSymbolKeyedMap(properties));
            return this;
        }

        public TargetBuilder withCapabilities(String... capabilities) {
            target.setCapabilities(TypeMapper.toSymbolArray(capabilities));
            return this;
        }

        public TargetBuilder withCapabilities(Symbol... capabilities) {
            target.setCapabilities(capabilities);
            return this;
        }
    }

    public final class CoordinatorBuilder extends TerminusBuilder {

        private final Coordinator coordinator;

        public CoordinatorBuilder(Coordinator coordinator) {
            this.coordinator = coordinator;
        }

        public CoordinatorBuilder withCapabilities(String... capabilities) {
            coordinator.setCapabilities(TypeMapper.toSymbolArray(capabilities));
            return this;
        }

        public CoordinatorBuilder withCapabilities(Symbol... capabilities) {
            coordinator.setCapabilities(capabilities);
            return this;
        }
    }
}

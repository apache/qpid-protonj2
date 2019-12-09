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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.DescribedType;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.amqp.driver.codec.transport.Attach;
import org.apache.qpid.proton4j.amqp.driver.codec.util.TypeMapper;
import org.apache.qpid.proton4j.amqp.messaging.Outcome;
import org.apache.qpid.proton4j.amqp.messaging.Source;
import org.apache.qpid.proton4j.amqp.messaging.Target;
import org.apache.qpid.proton4j.amqp.messaging.TerminusDurability;
import org.apache.qpid.proton4j.amqp.messaging.TerminusExpiryPolicy;
import org.apache.qpid.proton4j.amqp.transport.DeliveryState;
import org.apache.qpid.proton4j.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton4j.amqp.transport.Role;
import org.apache.qpid.proton4j.amqp.transport.SenderSettleMode;

/**
 * AMQP Attach injection action which can be added to a driver for write at a specific time or
 * following on from some other action in the test script.
 */
public class AttachInjectAction extends AbstractPerformativeInjectAction<Attach> {

    private final Attach attach = new Attach();

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
        attach.setName(name);
        return this;
    }

    public AttachInjectAction withHandle(long handle) {
        attach.setHandle(UnsignedInteger.valueOf(handle));
        return this;
    }

    public AttachInjectAction withHandle(UnsignedInteger handle) {
        attach.setHandle(handle);
        return this;
    }

    public AttachInjectAction withRole(Role role) {
        attach.setRole(role.getValue());
        return this;
    }

    public AttachInjectAction withSndSettleMode(SenderSettleMode sndSettleMode) {
        attach.setSndSettleMode(sndSettleMode == null ? null : sndSettleMode.getValue());
        return this;
    }

    public AttachInjectAction withRcvSettleMode(ReceiverSettleMode rcvSettleMode) {
        attach.setRcvSettleMode(rcvSettleMode == null ? null : rcvSettleMode.getValue());
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
        return new SourceBuilder(getOrCreateSouce());
    }

    public AttachInjectAction withSource(Source source) {
        nullSourceRequired = source == null;
        attach.setSource(TypeMapper.mapFromProtonType(source));
        return this;
    }

    public AttachInjectAction withSource(org.apache.qpid.proton4j.amqp.driver.codec.messaging.Source source) {
        nullSourceRequired = source == null;
        attach.setSource(source);
        return this;
    }

    public boolean isNullTargetRequired() {
        return nullTargetRequired;
    }

    public AttachInjectAction withNullTarget() {
        nullTargetRequired = true;
        attach.setTarget(null);
        return this;
    }

    public TargetBuilder withTarget() {
        nullSourceRequired = false;
        return new TargetBuilder(getOrCreateTarget());
    }

    public AttachInjectAction withTarget(Target target) {
        nullTargetRequired = target == null;
        attach.setTarget(TypeMapper.mapFromProtonType(target));
        return this;
    }

    public AttachInjectAction withTarget(org.apache.qpid.proton4j.amqp.driver.codec.messaging.Target target) {
        nullTargetRequired = target == null;
        attach.setTarget(target);
        return this;
    }

    public AttachInjectAction withUnsettled(Map<Binary, DeliveryState> unsettled) {
        if (unsettled != null) {
            Map<Binary, DescribedType> converted = new LinkedHashMap<>();
            for (Entry<Binary, DeliveryState> entry : unsettled.entrySet()) {
                converted.put(entry.getKey(), TypeMapper.mapFromProtonType(entry.getValue()));
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

    public AttachInjectAction withOfferedCapabilities(Symbol... offeredCapabilities) {
        attach.setOfferedCapabilities(offeredCapabilities);
        return this;
    }

    public AttachInjectAction withDesiredCapabilities(Symbol... desiredCapabilities) {
        attach.setDesiredCapabilities(desiredCapabilities);
        return this;
    }

    public AttachInjectAction withProperties(Map<Symbol, Object> properties) {
        attach.setProperties(properties);
        return this;
    }

    @Override
    protected void beforeActionPerformed(AMQPTestDriver driver) {
        // We fill in a channel using the next available channel id if one isn't set, then
        // report the outbound begin to the session so it can track this new session.
        if (onChannel() == CHANNEL_UNSET) {
            onChannel(driver.getSessions().getLastOpenedSession().getLocalChannel().intValue());
        }
    }

    private org.apache.qpid.proton4j.amqp.driver.codec.messaging.Source getOrCreateSouce() {
        if (attach.getSource() == null) {
            attach.setSource(new org.apache.qpid.proton4j.amqp.driver.codec.messaging.Source());
        }
        return attach.getSource();
    }

    private org.apache.qpid.proton4j.amqp.driver.codec.messaging.Target getOrCreateTarget() {
        if (attach.getTarget() == null) {
            attach.setTarget(new org.apache.qpid.proton4j.amqp.driver.codec.messaging.Target());
        }
        return attach.getTarget();
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

        private final org.apache.qpid.proton4j.amqp.driver.codec.messaging.Source source;

        public SourceBuilder(org.apache.qpid.proton4j.amqp.driver.codec.messaging.Source source) {
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

        public SourceBuilder withExpiryPolicy(TerminusExpiryPolicy expriyPolicy) {
            source.setExpiryPolicy(expriyPolicy.getPolicy());
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

        public SourceBuilder withDynamicNodeProperties(Map<Symbol, Object> properties) {
            source.setDynamicNodeProperties(properties);
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

        public SourceBuilder withDefaultOutcome(Outcome outcome) {
            source.setDefaultOutcome(TypeMapper.mapFromProtonType(outcome));
            return this;
        }

        public SourceBuilder withCapabilities(String... capabilities) {
            source.setCapabilities(TypeMapper.toSymbolArray(capabilities));
            return this;
        }

        public SourceBuilder withOutcomes(Symbol... outcomes) {
            source.setOutcomes(outcomes);
            return this;
        }

        public SourceBuilder withCapabilities(Symbol... capabilities) {
            source.setCapabilities(capabilities);
            return this;
        }
    }

    public final class TargetBuilder extends TerminusBuilder {

        private final org.apache.qpid.proton4j.amqp.driver.codec.messaging.Target target;

        public TargetBuilder(org.apache.qpid.proton4j.amqp.driver.codec.messaging.Target target) {
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

        public TargetBuilder withExpiryPolicy(TerminusExpiryPolicy expriyPolicy) {
            target.setExpiryPolicy(expriyPolicy.getPolicy());
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

        public TargetBuilder withDynamicNodeProperties(Map<Symbol, Object> properties) {
            target.setDynamicNodeProperties(properties);
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
}

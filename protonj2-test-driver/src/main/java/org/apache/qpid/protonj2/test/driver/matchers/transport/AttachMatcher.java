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
package org.apache.qpid.protonj2.test.driver.matchers.transport;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;

import java.util.Map;

import org.apache.qpid.protonj2.test.driver.codec.messaging.Source;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Target;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Binary;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Symbol;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedInteger;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedLong;
import org.apache.qpid.protonj2.test.driver.codec.transactions.Coordinator;
import org.apache.qpid.protonj2.test.driver.codec.transport.Attach;
import org.apache.qpid.protonj2.test.driver.codec.transport.DeliveryState;
import org.apache.qpid.protonj2.test.driver.codec.transport.ReceiverSettleMode;
import org.apache.qpid.protonj2.test.driver.codec.transport.Role;
import org.apache.qpid.protonj2.test.driver.codec.transport.SenderSettleMode;
import org.apache.qpid.protonj2.test.driver.codec.util.TypeMapper;
import org.apache.qpid.protonj2.test.driver.matchers.ArrayContentsMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.ListDescribedTypeMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.MapContentsMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.SourceMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.TargetMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.transactions.CoordinatorMatcher;
import org.hamcrest.Matcher;

public class AttachMatcher extends ListDescribedTypeMatcher {

    // Only used if singular 'withProperty' API is used
    private MapContentsMatcher<Symbol, Object> propertiesMatcher;

    // Only used if singular 'withDesiredCapabilitiy' API is used
    private ArrayContentsMatcher<Symbol> desiredCapabilitiesMatcher;

    // Only used if singular 'withOfferedCapability' API is used
    private ArrayContentsMatcher<Symbol> offeredCapabilitiesMatcher;

    public AttachMatcher() {
        super(Attach.Field.values().length, Attach.DESCRIPTOR_CODE, Attach.DESCRIPTOR_SYMBOL);
    }

    @Override
    protected Class<?> getDescribedTypeClass() {
        return Attach.class;
    }

    //----- Type specific with methods that perform simple equals checks

    public AttachMatcher withName(String name) {
        return withName(equalTo(name));
    }

    public AttachMatcher withHandle(int handle) {
        return withHandle(equalTo(UnsignedInteger.valueOf(handle)));
    }

    public AttachMatcher withHandle(long handle) {
        return withHandle(equalTo(UnsignedInteger.valueOf(handle)));
    }

    public AttachMatcher withHandle(UnsignedInteger handle) {
        return withHandle(equalTo(handle));
    }

    public AttachMatcher withRole(boolean role) {
        return withRole(equalTo(role));
    }

    public AttachMatcher withRole(Boolean role) {
        return withRole(equalTo(role));
    }

    public AttachMatcher withRole(Role role) {
        return withRole(equalTo(role.getValue()));
    }

    public AttachMatcher withSndSettleMode(byte sndSettleMode) {
        return withSndSettleMode(equalTo(SenderSettleMode.valueOf(sndSettleMode).getValue()));
    }

    public AttachMatcher withSndSettleMode(Byte sndSettleMode) {
        return withSndSettleMode(sndSettleMode == null ? nullValue() : equalTo(SenderSettleMode.valueOf(sndSettleMode).getValue()));
    }

    public AttachMatcher withSndSettleMode(SenderSettleMode sndSettleMode) {
        return withSndSettleMode(sndSettleMode == null ? nullValue() : equalTo(sndSettleMode.getValue()));
    }

    public AttachMatcher withRcvSettleMode(byte rcvSettleMode) {
        return withRcvSettleMode(equalTo(ReceiverSettleMode.valueOf(rcvSettleMode).getValue()));
    }

    public AttachMatcher withRcvSettleMode(Byte rcvSettleMode) {
        return withRcvSettleMode(rcvSettleMode == null ? nullValue() : equalTo(ReceiverSettleMode.valueOf(rcvSettleMode).getValue()));
    }

    public AttachMatcher withRcvSettleMode(ReceiverSettleMode rcvSettleMode) {
        return withRcvSettleMode(rcvSettleMode == null ? nullValue() : equalTo(rcvSettleMode.getValue()));
    }

    public AttachMatcher withSource(Source source) {
        if (source != null) {
            SourceMatcher sourceMatcher = new SourceMatcher(source);
            return withSource(sourceMatcher);
        } else {
            return withSource(nullValue());
        }
    }

    public AttachMatcher withTarget(Target target) {
        if (target != null) {
            TargetMatcher targetMatcher = new TargetMatcher(target);
            return withTarget(targetMatcher);
        } else {
            return withTarget(nullValue());
        }
    }

    public AttachMatcher withCoordinator(Coordinator coordinator) {
        if (coordinator != null) {
            CoordinatorMatcher coordinatorMatcher = new CoordinatorMatcher();
            return withCoordinator(coordinatorMatcher);
        } else {
            return withCoordinator(nullValue());
        }
    }

    public AttachMatcher withUnsettled(Map<Binary, DeliveryState> unsettled) {
        // TODO - Need to match on the driver types for DeliveryState
        return withUnsettled(equalTo(unsettled));
    }

    public AttachMatcher withIncompleteUnsettled(boolean incomplete) {
        return withIncompleteUnsettled(equalTo(incomplete));
    }

    public AttachMatcher withInitialDeliveryCount(int initialDeliveryCount) {
        return withInitialDeliveryCount(equalTo(UnsignedInteger.valueOf(initialDeliveryCount)));
    }

    public AttachMatcher withInitialDeliveryCount(long initialDeliveryCount) {
        return withInitialDeliveryCount(equalTo(UnsignedInteger.valueOf(initialDeliveryCount)));
    }

    public AttachMatcher withInitialDeliveryCount(UnsignedInteger initialDeliveryCount) {
        return withInitialDeliveryCount(equalTo(initialDeliveryCount));
    }

    public AttachMatcher withMaxMessageSize(long maxMessageSize) {
        return withMaxMessageSize(equalTo(UnsignedLong.valueOf(maxMessageSize)));
    }

    public AttachMatcher withMaxMessageSize(UnsignedLong maxMessageSize) {
        return withMaxMessageSize(equalTo(maxMessageSize));
    }

    public AttachMatcher withOfferedCapabilities(Symbol... offeredCapabilities) {
        offeredCapabilitiesMatcher = null; // Clear these as this overrides anything else
        return withOfferedCapabilities(equalTo(offeredCapabilities));
    }

    public AttachMatcher withOfferedCapabilities(String... offeredCapabilities) {
        offeredCapabilitiesMatcher = null; // Clear these as this overrides anything else
        return withOfferedCapabilities(equalTo(TypeMapper.toSymbolArray(offeredCapabilities)));
    }

    public AttachMatcher withDesiredCapabilities(Symbol... desiredCapabilities) {
        desiredCapabilitiesMatcher = null; // Clear these as this overrides anything else
        return withDesiredCapabilities(equalTo(desiredCapabilities));
    }

    public AttachMatcher withDesiredCapabilities(String... desiredCapabilities) {
        desiredCapabilitiesMatcher = null; // Clear these as this overrides anything else
        return withDesiredCapabilities(equalTo(TypeMapper.toSymbolArray(desiredCapabilities)));
    }

    public AttachMatcher withPropertiesMap(Map<Symbol, Object> properties) {
        propertiesMatcher = null; // Clear these as this overrides anything else
        return withProperties(equalTo(properties));
    }

    public AttachMatcher withProperties(Map<String, Object> properties) {
        return withPropertiesMap(TypeMapper.toSymbolKeyedMap(properties));
    }

    public AttachMatcher withProperty(String key, Object value) {
        return withProperty(Symbol.valueOf(key), value);
    }

    public AttachMatcher withProperty(Symbol key, Object value) {
        if (propertiesMatcher == null) {
            propertiesMatcher = new MapContentsMatcher<Symbol, Object>();
        }

        propertiesMatcher.addExpectedEntry(key, value);

        return withProperties(propertiesMatcher);
    }

    public AttachMatcher withDesiredCapability(String value) {
        return withDesiredCapability(Symbol.valueOf(value));
    }

    public AttachMatcher withDesiredCapability(Symbol value) {
        if (desiredCapabilitiesMatcher == null) {
            desiredCapabilitiesMatcher = new ArrayContentsMatcher<Symbol>();
        }

        desiredCapabilitiesMatcher.addExpectedEntry(value);

        return withDesiredCapabilities(desiredCapabilitiesMatcher);
    }

    public AttachMatcher withOfferedCapability(String value) {
        return withOfferedCapability(Symbol.valueOf(value));
    }

    public AttachMatcher withOfferedCapability(Symbol value) {
        if (offeredCapabilitiesMatcher == null) {
            offeredCapabilitiesMatcher = new ArrayContentsMatcher<Symbol>();
        }

        offeredCapabilitiesMatcher.addExpectedEntry(value);

        return withOfferedCapabilities(offeredCapabilitiesMatcher);
    }

    //----- Matcher based with methods for more complex validation

    public AttachMatcher withName(Matcher<?> m) {
        addFieldMatcher(Attach.Field.NAME, m);
        return this;
    }

    public AttachMatcher withHandle(Matcher<?> m) {
        addFieldMatcher(Attach.Field.HANDLE, m);
        return this;
    }

    public AttachMatcher withRole(Matcher<?> m) {
        addFieldMatcher(Attach.Field.ROLE, m);
        return this;
    }

    public AttachMatcher withSndSettleMode(Matcher<?> m) {
        addFieldMatcher(Attach.Field.SND_SETTLE_MODE, m);
        return this;
    }

    public AttachMatcher withRcvSettleMode(Matcher<?> m) {
        addFieldMatcher(Attach.Field.RCV_SETTLE_MODE, m);
        return this;
    }

    public AttachMatcher withSource(Matcher<?> m) {
        addFieldMatcher(Attach.Field.SOURCE, m);
        return this;
    }

    public AttachMatcher withTarget(Matcher<?> m) {
        addFieldMatcher(Attach.Field.TARGET, m);
        return this;
    }

    public AttachMatcher withCoordinator(Matcher<?> m) {
        addFieldMatcher(Attach.Field.TARGET, m);
        return this;
    }

    public AttachMatcher withUnsettled(Matcher<?> m) {
        addFieldMatcher(Attach.Field.UNSETTLED, m);
        return this;
    }

    public AttachMatcher withIncompleteUnsettled(Matcher<?> m) {
        addFieldMatcher(Attach.Field.INCOMPLETE_UNSETTLED, m);
        return this;
    }

    public AttachMatcher withInitialDeliveryCount(Matcher<?> m) {
        addFieldMatcher(Attach.Field.INITIAL_DELIVERY_COUNT, m);
        return this;
    }

    public AttachMatcher withMaxMessageSize(Matcher<?> m) {
        addFieldMatcher(Attach.Field.MAX_MESSAGE_SIZE, m);
        return this;
    }

    public AttachMatcher withOfferedCapabilities(Matcher<?> m) {
        addFieldMatcher(Attach.Field.OFFERED_CAPABILITIES, m);
        return this;
    }

    public AttachMatcher withDesiredCapabilities(Matcher<?> m) {
        addFieldMatcher(Attach.Field.DESIRED_CAPABILITIES, m);
        return this;
    }

    public AttachMatcher withProperties(Matcher<?> m) {
        addFieldMatcher(Attach.Field.PROPERTIES, m);
        return this;
    }
}

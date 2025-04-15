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

import java.util.Map;

import org.apache.qpid.protonj2.test.driver.codec.primitives.Symbol;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedInteger;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedShort;
import org.apache.qpid.protonj2.test.driver.codec.transport.Open;
import org.apache.qpid.protonj2.test.driver.codec.util.TypeMapper;
import org.apache.qpid.protonj2.test.driver.matchers.ArrayContentsMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.ListDescribedTypeMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.MapContentsMatcher;
import org.hamcrest.Matcher;

public class OpenMatcher extends ListDescribedTypeMatcher {

    // Only used if singular 'withProperty' API is used
    private MapContentsMatcher<Symbol, Object> propertiesMatcher;

    // Only used if singular 'withDesiredCapabilitiy' API is used
    private ArrayContentsMatcher<Symbol> desiredCapabilitiesMatcher;

    // Only used if singular 'withOfferedCapability' API is used
    private ArrayContentsMatcher<Symbol> offeredCapabilitiesMatcher;

    public OpenMatcher() {
        super(Open.Field.values().length, Open.DESCRIPTOR_CODE, Open.DESCRIPTOR_SYMBOL);
    }

    @Override
    protected Class<?> getDescribedTypeClass() {
        return Open.class;
    }

    //----- Type specific with methods that perform simple equals checks

    public OpenMatcher withContainerId(String container) {
        return withContainerId(equalTo(container));
    }

    public OpenMatcher withHostname(String hostname) {
        return withHostname(equalTo(hostname));
    }

    public OpenMatcher withMaxFrameSize(int maxFrameSize) {
        return withMaxFrameSize(equalTo(UnsignedInteger.valueOf(maxFrameSize)));
    }

    public OpenMatcher withMaxFrameSize(long maxFrameSize) {
        return withMaxFrameSize(equalTo(UnsignedInteger.valueOf(maxFrameSize)));
    }

    public OpenMatcher withMaxFrameSize(UnsignedInteger maxFrameSize) {
        return withMaxFrameSize(equalTo(maxFrameSize));
    }

    public OpenMatcher withChannelMax(short channelMax) {
        return withChannelMax(equalTo(UnsignedShort.valueOf(channelMax)));
    }

    public OpenMatcher withChannelMax(int channelMax) {
        return withChannelMax(equalTo(UnsignedShort.valueOf(channelMax)));
    }

    public OpenMatcher withChannelMax(UnsignedShort channelMax) {
        return withChannelMax(equalTo(channelMax));
    }

    public OpenMatcher withIdleTimeOut(int idleTimeout) {
        return withIdleTimeOut(equalTo(UnsignedInteger.valueOf(idleTimeout)));
    }

    public OpenMatcher withIdleTimeOut(long idleTimeout) {
        return withIdleTimeOut(equalTo(UnsignedInteger.valueOf(idleTimeout)));
    }

    public OpenMatcher withIdleTimeOut(UnsignedInteger idleTimeout) {
        return withIdleTimeOut(equalTo(idleTimeout));
    }

    public OpenMatcher withOutgoingLocales(String... outgoingLocales) {
        return withOutgoingLocales(equalTo(TypeMapper.toSymbolArray(outgoingLocales)));
    }

    public OpenMatcher withOutgoingLocales(Symbol... outgoingLocales) {
        return withOutgoingLocales(equalTo(outgoingLocales));
    }

    public OpenMatcher withIncomingLocales(String... incomingLocales) {
        return withIncomingLocales(equalTo(TypeMapper.toSymbolArray(incomingLocales)));
    }

    public OpenMatcher withIncomingLocales(Symbol... incomingLocales) {
        return withIncomingLocales(equalTo(incomingLocales));
    }

    public OpenMatcher withOfferedCapabilities(String... offeredCapabilities) {
        offeredCapabilitiesMatcher = null; // Clear these as this overrides anything else
        return withOfferedCapabilities(equalTo(TypeMapper.toSymbolArray(offeredCapabilities)));
    }

    public OpenMatcher withOfferedCapabilities(Symbol... offeredCapabilities) {
        offeredCapabilitiesMatcher = null; // Clear these as this overrides anything else
        return withOfferedCapabilities(equalTo(offeredCapabilities));
    }

    public OpenMatcher withDesiredCapabilities(String... desiredCapabilities) {
        desiredCapabilitiesMatcher = null; // Clear these as this overrides anything else
        return withDesiredCapabilities(equalTo(TypeMapper.toSymbolArray(desiredCapabilities)));
    }

    public OpenMatcher withDesiredCapabilities(Symbol... desiredCapabilities) {
        desiredCapabilitiesMatcher = null; // Clear these as this overrides anything else
        return withDesiredCapabilities(equalTo(desiredCapabilities));
    }

    public OpenMatcher withPropertiesMap(Map<Symbol, Object> properties) {
        propertiesMatcher = null; // Clear these as this overrides anything else
        return withProperties(equalTo(properties));
    }

    public OpenMatcher withProperties(Map<String, Object> properties) {
        return withPropertiesMap(TypeMapper.toSymbolKeyedMap(properties));
    }

    public OpenMatcher withProperty(String key, Object value) {
        return withProperty(Symbol.valueOf(key), value);
    }

    public OpenMatcher withProperty(Symbol key, Object value) {
        if (propertiesMatcher == null) {
            propertiesMatcher = new MapContentsMatcher<>();
        }

        propertiesMatcher.addExpectedEntry(key, value);

        return withProperties(propertiesMatcher);
    }

    public OpenMatcher withDesiredCapability(String value) {
        return withDesiredCapability(Symbol.valueOf(value));
    }

    public OpenMatcher withDesiredCapability(Symbol value) {
        if (desiredCapabilitiesMatcher == null) {
            desiredCapabilitiesMatcher = new ArrayContentsMatcher<Symbol>();
        }

        desiredCapabilitiesMatcher.addExpectedEntry(value);

        return withDesiredCapabilities(desiredCapabilitiesMatcher);
    }

    public OpenMatcher withOfferedCapability(String value) {
        return withOfferedCapability(Symbol.valueOf(value));
    }

    public OpenMatcher withOfferedCapability(Symbol value) {
        if (offeredCapabilitiesMatcher == null) {
            offeredCapabilitiesMatcher = new ArrayContentsMatcher<Symbol>();
        }

        offeredCapabilitiesMatcher.addExpectedEntry(value);

        return withOfferedCapabilities(offeredCapabilitiesMatcher);
    }

    //----- Matcher based with methods for more complex validation

    public OpenMatcher withContainerId(Matcher<?> m) {
        addFieldMatcher(Open.Field.CONTAINER_ID, m);
        return this;
    }

    public OpenMatcher withHostname(Matcher<?> m) {
        addFieldMatcher(Open.Field.HOSTNAME, m);
        return this;
    }

    public OpenMatcher withMaxFrameSize(Matcher<?> m) {
        addFieldMatcher(Open.Field.MAX_FRAME_SIZE, m);
        return this;
    }

    public OpenMatcher withChannelMax(Matcher<?> m) {
        addFieldMatcher(Open.Field.CHANNEL_MAX, m);
        return this;
    }

    public OpenMatcher withIdleTimeOut(Matcher<?> m) {
        addFieldMatcher(Open.Field.IDLE_TIME_OUT, m);
        return this;
    }

    public OpenMatcher withOutgoingLocales(Matcher<?> m) {
        addFieldMatcher(Open.Field.OUTGOING_LOCALES, m);
        return this;
    }

    public OpenMatcher withIncomingLocales(Matcher<?> m) {
        addFieldMatcher(Open.Field.INCOMING_LOCALES, m);
        return this;
    }

    public OpenMatcher withOfferedCapabilities(Matcher<?> m) {
        addFieldMatcher(Open.Field.OFFERED_CAPABILITIES, m);
        return this;
    }

    public OpenMatcher withDesiredCapabilities(Matcher<?> m) {
        addFieldMatcher(Open.Field.DESIRED_CAPABILITIES, m);
        return this;
    }

    public OpenMatcher withProperties(Matcher<?> m) {
        addFieldMatcher(Open.Field.PROPERTIES, m);
        return this;
    }
}

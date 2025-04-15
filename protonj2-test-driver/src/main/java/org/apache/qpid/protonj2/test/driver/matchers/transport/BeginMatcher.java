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
import org.apache.qpid.protonj2.test.driver.codec.transport.Begin;
import org.apache.qpid.protonj2.test.driver.codec.util.TypeMapper;
import org.apache.qpid.protonj2.test.driver.matchers.ArrayContentsMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.ListDescribedTypeMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.MapContentsMatcher;
import org.hamcrest.Matcher;

public class BeginMatcher extends ListDescribedTypeMatcher {

    // Only used if singular 'withProperty' API is used
    private MapContentsMatcher<Symbol, Object> propertiesMatcher;

    // Only used if singular 'withDesiredCapabilitiy' API is used
    private ArrayContentsMatcher<Symbol> desiredCapabilitiesMatcher;

    // Only used if singular 'withOfferedCapability' API is used
    private ArrayContentsMatcher<Symbol> offeredCapabilitiesMatcher;

    public BeginMatcher() {
        super(Begin.Field.values().length, Begin.DESCRIPTOR_CODE, Begin.DESCRIPTOR_SYMBOL);
    }

    @Override
    protected Class<?> getDescribedTypeClass() {
        return Begin.class;
    }

    //----- Type specific with methods that perform simple equals checks

    public BeginMatcher withRemoteChannel(int remoteChannel) {
        return withRemoteChannel(equalTo(UnsignedShort.valueOf((short) remoteChannel)));
    }

    public BeginMatcher withRemoteChannel(UnsignedShort remoteChannel) {
        return withRemoteChannel(equalTo(remoteChannel));
    }

    public BeginMatcher withNextOutgoingId(int nextOutgoingId) {
        return withNextOutgoingId(equalTo(UnsignedInteger.valueOf(nextOutgoingId)));
    }

    public BeginMatcher withNextOutgoingId(long nextOutgoingId) {
        return withNextOutgoingId(equalTo(UnsignedInteger.valueOf(nextOutgoingId)));
    }

    public BeginMatcher withNextOutgoingId(UnsignedInteger nextOutgoingId) {
        return withNextOutgoingId(equalTo(nextOutgoingId));
    }

    public BeginMatcher withIncomingWindow(int incomingWindow) {
        return withIncomingWindow(equalTo(UnsignedInteger.valueOf(incomingWindow)));
    }

    public BeginMatcher withIncomingWindow(long incomingWindow) {
        return withIncomingWindow(equalTo(UnsignedInteger.valueOf(incomingWindow)));
    }

    public BeginMatcher withIncomingWindow(UnsignedInteger incomingWindow) {
        return withIncomingWindow(equalTo(incomingWindow));
    }

    public BeginMatcher withOutgoingWindow(int outgoingWindow) {
        return withOutgoingWindow(equalTo(UnsignedInteger.valueOf(outgoingWindow)));
    }

    public BeginMatcher withOutgoingWindow(long outgoingWindow) {
        return withOutgoingWindow(equalTo(UnsignedInteger.valueOf(outgoingWindow)));
    }

    public BeginMatcher withOutgoingWindow(UnsignedInteger outgoingWindow) {
        return withOutgoingWindow(equalTo(outgoingWindow));
    }

    public BeginMatcher withHandleMax(int handleMax) {
        return withHandleMax(equalTo(UnsignedInteger.valueOf(handleMax)));
    }

    public BeginMatcher withHandleMax(long handleMax) {
        return withHandleMax(equalTo(UnsignedInteger.valueOf(handleMax)));
    }

    public BeginMatcher withHandleMax(UnsignedInteger handleMax) {
        return withHandleMax(equalTo(handleMax));
    }

    public BeginMatcher withOfferedCapabilities(String... offeredCapabilities) {
        offeredCapabilitiesMatcher = null; // Clear these as this overrides anything else
        return withOfferedCapabilities(equalTo(TypeMapper.toSymbolArray(offeredCapabilities)));
    }

    public BeginMatcher withOfferedCapabilities(Symbol... offeredCapabilities) {
        offeredCapabilitiesMatcher = null; // Clear these as this overrides anything else
        return withOfferedCapabilities(equalTo(offeredCapabilities));
    }

    public BeginMatcher withDesiredCapabilities(String... desiredCapabilities) {
        desiredCapabilitiesMatcher = null; // Clear these as this overrides anything else
        return withDesiredCapabilities(equalTo(TypeMapper.toSymbolArray(desiredCapabilities)));
    }

    public BeginMatcher withDesiredCapabilities(Symbol... desiredCapabilities) {
        desiredCapabilitiesMatcher = null; // Clear these as this overrides anything else
        return withDesiredCapabilities(equalTo(desiredCapabilities));
    }

    public BeginMatcher withPropertiesMap(Map<Symbol, Object> properties) {
        propertiesMatcher = null; // Clear these as this overrides anything else
        return withProperties(equalTo(properties));
    }

    public BeginMatcher withProperties(Map<String, Object> properties) {
        return withPropertiesMap(TypeMapper.toSymbolKeyedMap(properties));
    }

    public BeginMatcher withProperty(String key, Object value) {
        return withProperty(Symbol.valueOf(key), value);
    }

    public BeginMatcher withProperty(Symbol key, Object value) {
        if (propertiesMatcher == null) {
            propertiesMatcher = new MapContentsMatcher<>();
        }

        propertiesMatcher.addExpectedEntry(key, value);

        return withProperties(propertiesMatcher);
    }

    public BeginMatcher withDesiredCapability(String value) {
        return withDesiredCapability(Symbol.valueOf(value));
    }

    public BeginMatcher withDesiredCapability(Symbol value) {
        if (desiredCapabilitiesMatcher == null) {
            desiredCapabilitiesMatcher = new ArrayContentsMatcher<Symbol>();
        }

        desiredCapabilitiesMatcher.addExpectedEntry(value);

        return withDesiredCapabilities(desiredCapabilitiesMatcher);
    }

    public BeginMatcher withOfferedCapability(String value) {
        return withOfferedCapability(Symbol.valueOf(value));
    }

    public BeginMatcher withOfferedCapability(Symbol value) {
        if (offeredCapabilitiesMatcher == null) {
            offeredCapabilitiesMatcher = new ArrayContentsMatcher<Symbol>();
        }

        offeredCapabilitiesMatcher.addExpectedEntry(value);

        return withOfferedCapabilities(offeredCapabilitiesMatcher);
    }

    //----- Matcher based with methods for more complex validation

    public BeginMatcher withRemoteChannel(Matcher<?> m) {
        addFieldMatcher(Begin.Field.REMOTE_CHANNEL, m);
        return this;
    }

    public BeginMatcher withNextOutgoingId(Matcher<?> m) {
        addFieldMatcher(Begin.Field.NEXT_OUTGOING_ID, m);
        return this;
    }

    public BeginMatcher withIncomingWindow(Matcher<?> m) {
        addFieldMatcher(Begin.Field.INCOMING_WINDOW, m);
        return this;
    }

    public BeginMatcher withOutgoingWindow(Matcher<?> m) {
        addFieldMatcher(Begin.Field.OUTGOING_WINDOW, m);
        return this;
    }

    public BeginMatcher withHandleMax(Matcher<?> m) {
        addFieldMatcher(Begin.Field.HANDLE_MAX, m);
        return this;
    }

    public BeginMatcher withOfferedCapabilities(Matcher<?> m) {
        addFieldMatcher(Begin.Field.OFFERED_CAPABILITIES, m);
        return this;
    }

    public BeginMatcher withDesiredCapabilities(Matcher<?> m) {
        addFieldMatcher(Begin.Field.DESIRED_CAPABILITIES, m);
        return this;
    }

    public BeginMatcher withProperties(Matcher<?> m) {
        addFieldMatcher(Begin.Field.PROPERTIES, m);
        return this;
    }
}

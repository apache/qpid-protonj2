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
package org.apache.qpid.proton4j.test.driver.matchers.messaging;

import static org.hamcrest.CoreMatchers.equalTo;

import java.util.Map;

import org.apache.qpid.proton4j.test.driver.codec.primitives.Symbol;
import org.apache.qpid.proton4j.test.driver.codec.primitives.UnsignedInteger;
import org.apache.qpid.proton4j.test.driver.codec.primitives.UnsignedShort;
import org.apache.qpid.proton4j.test.driver.codec.transport.Open;
import org.apache.qpid.proton4j.test.driver.codec.util.TypeMapper;
import org.apache.qpid.proton4j.test.driver.matchers.ListDescribedTypeMatcher;
import org.hamcrest.Matcher;

public class OpenMatcher extends ListDescribedTypeMatcher {

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
        return withOfferedCapabilities(equalTo(TypeMapper.toSymbolArray(offeredCapabilities)));
    }

    public OpenMatcher withOfferedCapabilities(Symbol... offeredCapabilities) {
        return withOfferedCapabilities(equalTo(offeredCapabilities));
    }

    public OpenMatcher withDesiredCapabilities(String... desiredCapabilities) {
        return withDesiredCapabilities(equalTo(TypeMapper.toSymbolArray(desiredCapabilities)));
    }

    public OpenMatcher withDesiredCapabilities(Symbol... desiredCapabilities) {
        return withDesiredCapabilities(equalTo(desiredCapabilities));
    }

    public OpenMatcher withPropertiesMap(Map<Symbol, Object> properties) {
        return withProperties(equalTo(properties));
    }

    public OpenMatcher withProperties(Map<String, Object> properties) {
        return withProperties(equalTo(TypeMapper.toSymbolKeyedMap(properties)));
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

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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.collection.ArrayMatching.hasItemInArray;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.qpid.protonj2.test.driver.AMQPTestDriver;
import org.apache.qpid.protonj2.test.driver.actions.BeginInjectAction;
import org.apache.qpid.protonj2.test.driver.actions.CloseInjectAction;
import org.apache.qpid.protonj2.test.driver.actions.OpenInjectAction;
import org.apache.qpid.protonj2.test.driver.codec.ListDescribedType;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Symbol;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedInteger;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedShort;
import org.apache.qpid.protonj2.test.driver.codec.transport.Open;
import org.apache.qpid.protonj2.test.driver.codec.util.TypeMapper;
import org.apache.qpid.protonj2.test.driver.matchers.transport.OpenMatcher;
import org.hamcrest.Matcher;

/**
 * Scripted expectation for the AMQP Open performative
 */
public class OpenExpectation extends AbstractExpectation<Open> {

    private final OpenMatcher matcher = new OpenMatcher();

    private OpenInjectAction response;
    private boolean explicitlyNullContainerId;

    public OpenExpectation(AMQPTestDriver driver) {
        super(driver);

        // Validate mandatory field by default
        withContainerId(notNullValue());

        onChannel(0);  // Open must used channel zero.
    }

    public OpenInjectAction respond() {
        response = new OpenInjectAction(driver);
        driver.addScriptedElement(response);
        return response;
    }

    public CloseInjectAction reject() {
        response = new OpenInjectAction(driver);
        driver.addScriptedElement(response);

        CloseInjectAction closeAction = new CloseInjectAction(driver);
        driver.addScriptedElement(closeAction);

        return closeAction;
    }

    public CloseInjectAction reject(String condition, String description) {
        return reject(Symbol.valueOf(condition), description);
    }

    public CloseInjectAction reject(String condition, String description, Map<String, Object> infoMap) {
        return reject(Symbol.valueOf(condition), description, TypeMapper.toSymbolKeyedMap(infoMap));
    }

    public CloseInjectAction reject(Symbol condition, String description) {
        return reject(condition, description, null);
    }

    public CloseInjectAction reject(Symbol condition, String description, Map<Symbol, Object> infoMap) {
        response = new OpenInjectAction(driver);
        driver.addScriptedElement(response);

        CloseInjectAction closeAction = new CloseInjectAction(driver).withErrorCondition(condition, description, infoMap);
        driver.addScriptedElement(closeAction);

        return closeAction;
    }

    //----- Handle the performative and configure response is told to respond

    @Override
    public void handleOpen(int frameSize, Open open, ByteBuffer payload, int channel, AMQPTestDriver context) {
        super.handleOpen(frameSize, open, payload, channel, context);

        if (response != null) {
            // Input was validated now populate response with auto values where not configured
            // to say otherwise by the test.
            if (response.getPerformative().getContainerId() == null && !explicitlyNullContainerId) {
                response.getPerformative().setContainerId("driver");
            }

            if (response.onChannel() == BeginInjectAction.CHANNEL_UNSET) {
                response.onChannel(channel);
            }
        }
    }

    //----- Type specific with methods that perform simple equals checks

    public OpenExpectation withContainerId(String container) {
        explicitlyNullContainerId = container == null;
        return withContainerId(equalTo(container));
    }

    public OpenExpectation withHostname(String hostname) {
        return withHostname(equalTo(hostname));
    }

    public OpenExpectation withMaxFrameSize(int maxFrameSize) {
        return withMaxFrameSize(equalTo(UnsignedInteger.valueOf(maxFrameSize)));
    }

    public OpenExpectation withMaxFrameSize(long maxFrameSize) {
        return withMaxFrameSize(equalTo(UnsignedInteger.valueOf(maxFrameSize)));
    }

    public OpenExpectation withMaxFrameSize(UnsignedInteger maxFrameSize) {
        return withMaxFrameSize(equalTo(maxFrameSize));
    }

    public OpenExpectation withChannelMax(short channelMax) {
        return withChannelMax(equalTo(UnsignedShort.valueOf(channelMax)));
    }

    public OpenExpectation withChannelMax(int channelMax) {
        return withChannelMax(equalTo(UnsignedShort.valueOf(channelMax)));
    }

    public OpenExpectation withChannelMax(UnsignedShort channelMax) {
        return withChannelMax(equalTo(channelMax));
    }

    public OpenExpectation withIdleTimeOut(int idleTimeout) {
        return withIdleTimeOut(equalTo(UnsignedInteger.valueOf(idleTimeout)));
    }

    public OpenExpectation withIdleTimeOut(long idleTimeout) {
        return withIdleTimeOut(equalTo(UnsignedInteger.valueOf(idleTimeout)));
    }

    public OpenExpectation withIdleTimeOut(UnsignedInteger idleTimeout) {
        return withIdleTimeOut(equalTo(idleTimeout));
    }

    public OpenExpectation withOutgoingLocales(String... outgoingLocales) {
        return withOutgoingLocales(equalTo(TypeMapper.toSymbolArray(outgoingLocales)));
    }

    public OpenExpectation withOutgoingLocales(Symbol... outgoingLocales) {
        return withOutgoingLocales(equalTo(outgoingLocales));
    }

    public OpenExpectation withOutgoingLocale(String outgoingLocale) {
        return withOutgoingLocales(hasItemInArray(Symbol.valueOf(outgoingLocale)));
    }

    public OpenExpectation withOutgoingLocale(Symbol outgoingLocale) {
        return withOutgoingLocales(hasItemInArray(outgoingLocale));
    }

    public OpenExpectation withIncomingLocales(String... incomingLocales) {
        return withIncomingLocales(equalTo(TypeMapper.toSymbolArray(incomingLocales)));
    }

    public OpenExpectation withIncomingLocales(Symbol... incomingLocales) {
        return withIncomingLocales(equalTo(incomingLocales));
    }

    public OpenExpectation withIncomingLocale(String incomingLocale) {
        return withIncomingLocales(hasItemInArray(Symbol.valueOf(incomingLocale)));
    }

    public OpenExpectation withIncomingLocale(Symbol incomingLocale) {
        return withIncomingLocales(hasItemInArray(incomingLocale));
    }

    public OpenExpectation withOfferedCapabilities(String... offeredCapabilities) {
        return withOfferedCapabilities(equalTo(TypeMapper.toSymbolArray(offeredCapabilities)));
    }

    public OpenExpectation withOfferedCapabilities(Symbol... offeredCapabilities) {
        return withOfferedCapabilities(equalTo(offeredCapabilities));
    }

    public OpenExpectation withOfferedCapability(Symbol offeredCapability) {
        return withOfferedCapabilities(hasItemInArray(offeredCapability));
    }

    public OpenExpectation withOfferedCapability(String offeredCapability) {
        return withOfferedCapabilities(hasItemInArray(Symbol.valueOf(offeredCapability)));
    }

    public OpenExpectation withDesiredCapabilities(String... desiredCapabilities) {
        return withDesiredCapabilities(equalTo(TypeMapper.toSymbolArray(desiredCapabilities)));
    }

    public OpenExpectation withDesiredCapabilities(Symbol... desiredCapabilities) {
        return withDesiredCapabilities(equalTo(desiredCapabilities));
    }

    public OpenExpectation withDesiredCapability(Symbol desiredCapability) {
        return withDesiredCapabilities(hasItemInArray(desiredCapability));
    }

    public OpenExpectation withDesiredCapability(String desiredCapability) {
        return withDesiredCapabilities(hasItemInArray(Symbol.valueOf(desiredCapability)));
    }

    public OpenExpectation withPropertiesMap(Map<Symbol, Object> properties) {
        return withProperties(equalTo(properties));
    }

    public OpenExpectation withProperties(Map<String, Object> properties) {
        return withProperties(equalTo(TypeMapper.toSymbolKeyedMap(properties)));
    }

    //----- Matcher based with methods for more complex validation

    public OpenExpectation withContainerId(Matcher<?> m) {
        matcher.addFieldMatcher(Open.Field.CONTAINER_ID, m);
        return this;
    }

    public OpenExpectation withHostname(Matcher<?> m) {
        matcher.addFieldMatcher(Open.Field.HOSTNAME, m);
        return this;
    }

    public OpenExpectation withMaxFrameSize(Matcher<?> m) {
        matcher.addFieldMatcher(Open.Field.MAX_FRAME_SIZE, m);
        return this;
    }

    public OpenExpectation withChannelMax(Matcher<?> m) {
        matcher.addFieldMatcher(Open.Field.CHANNEL_MAX, m);
        return this;
    }

    public OpenExpectation withIdleTimeOut(Matcher<?> m) {
        matcher.addFieldMatcher(Open.Field.IDLE_TIME_OUT, m);
        return this;
    }

    public OpenExpectation withOutgoingLocales(Matcher<?> m) {
        matcher.addFieldMatcher(Open.Field.OUTGOING_LOCALES, m);
        return this;
    }

    public OpenExpectation withIncomingLocales(Matcher<?> m) {
        matcher.addFieldMatcher(Open.Field.INCOMING_LOCALES, m);
        return this;
    }

    public OpenExpectation withOfferedCapabilities(Matcher<?> m) {
        matcher.addFieldMatcher(Open.Field.OFFERED_CAPABILITIES, m);
        return this;
    }

    public OpenExpectation withDesiredCapabilities(Matcher<?> m) {
        matcher.addFieldMatcher(Open.Field.DESIRED_CAPABILITIES, m);
        return this;
    }

    public OpenExpectation withProperties(Matcher<?> m) {
        matcher.addFieldMatcher(Open.Field.PROPERTIES, m);
        return this;
    }

    @Override
    protected Matcher<ListDescribedType> getExpectationMatcher() {
        return matcher;
    }

    @Override
    protected Class<Open> getExpectedTypeClass() {
        return Open.class;
    }
}

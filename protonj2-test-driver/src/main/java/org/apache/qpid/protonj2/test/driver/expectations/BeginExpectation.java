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
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.apache.qpid.protonj2.test.driver.AMQPTestDriver;
import org.apache.qpid.protonj2.test.driver.actions.BeginInjectAction;
import org.apache.qpid.protonj2.test.driver.actions.EndInjectAction;
import org.apache.qpid.protonj2.test.driver.codec.ListDescribedType;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Symbol;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedInteger;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedShort;
import org.apache.qpid.protonj2.test.driver.codec.transport.Begin;
import org.apache.qpid.protonj2.test.driver.matchers.transport.BeginMatcher;
import org.hamcrest.Matcher;

/**
 * Scripted expectation for the AMQP Begin performative
 */
public class BeginExpectation extends AbstractExpectation<Begin> {

    private final BeginMatcher matcher = new BeginMatcher();

    private BeginInjectAction response;

    public BeginExpectation(AMQPTestDriver driver) {
        super(driver);

        // Configure default expectations for mandatory fields
        withRemoteChannel(anyOf(nullValue(), notNullValue()));
        withNextOutgoingId(notNullValue());
        withIncomingWindow(notNullValue());
        withOutgoingWindow(notNullValue());
    }

    @Override
    public BeginExpectation withPredicate(Predicate<Begin> predicate) {
        withPredicate(predicate);
        return this;
    }

    @Override
    public BeginExpectation withCapture(Consumer<Begin> capture) {
        withCapture(capture);
        return this;
    }

    @Override
    public BeginExpectation onChannel(int channel) {
        super.onChannel(channel);
        return this;
    }

    @Override
    public BeginExpectation optional() {
        super.optional();
        return this;
    }

    public BeginInjectAction respond() {
        response = new BeginInjectAction(driver);
        driver.addScriptedElement(response);
        return response;
    }

    public EndInjectAction reject(String condition, String description) {
        return reject(Symbol.valueOf(condition), description);
    }

    public EndInjectAction reject(Symbol condition, String description) {
        response = new BeginInjectAction(driver);
        driver.addScriptedElement(response);

        EndInjectAction endAction = new EndInjectAction(driver).withErrorCondition(condition, description);
        driver.addScriptedElement(endAction);

        return endAction;
    }

    //----- Handle the performative and configure response is told to respond

    @Override
    public void handleBegin(int frameSize, Begin begin, ByteBuffer payload, int channel, AMQPTestDriver context) {
        super.handleBegin(frameSize, begin, payload, channel, context);

        context.sessions().handleBegin(begin, UnsignedShort.valueOf(channel));

        if (response != null) {
            response.withRemoteChannel(channel);
        }
    }

    //----- Type specific with methods that perform simple equals checks

    public BeginExpectation withRemoteChannel(int remoteChannel) {
        matcher.withRemoteChannel(remoteChannel);
        return this;
    }

    public BeginExpectation withRemoteChannel(UnsignedShort remoteChannel) {
        matcher.withRemoteChannel(remoteChannel);
        return this;
    }

    public BeginExpectation withNextOutgoingId(int nextOutgoingId) {
        matcher.withNextOutgoingId(nextOutgoingId);
        return this;
    }

    public BeginExpectation withNextOutgoingId(long nextOutgoingId) {
        matcher.withNextOutgoingId(nextOutgoingId);
        return this;
    }

    public BeginExpectation withNextOutgoingId(UnsignedInteger nextOutgoingId) {
        matcher.withNextOutgoingId(nextOutgoingId);
        return this;
    }

    public BeginExpectation withIncomingWindow(int incomingWindow) {
        matcher.withIncomingWindow(incomingWindow);
        return this;
    }

    public BeginExpectation withIncomingWindow(long incomingWindow) {
        matcher.withIncomingWindow(incomingWindow);
        return this;
    }

    public BeginExpectation withIncomingWindow(UnsignedInteger incomingWindow) {
        matcher.withIncomingWindow(incomingWindow);
        return this;
    }

    public BeginExpectation withOutgoingWindow(int outgoingWindow) {
        matcher.withOutgoingWindow(outgoingWindow);
        return this;
    }

    public BeginExpectation withOutgoingWindow(long outgoingWindow) {
        matcher.withOutgoingWindow(outgoingWindow);
        return this;
    }

    public BeginExpectation withOutgoingWindow(UnsignedInteger outgoingWindow) {
        matcher.withOutgoingWindow(outgoingWindow);
        return this;
    }

    public BeginExpectation withHandleMax(int handleMax) {
        matcher.withHandleMax(handleMax);
        return this;
    }

    public BeginExpectation withHandleMax(long handleMax) {
        matcher.withHandleMax(handleMax);
        return this;
    }

    public BeginExpectation withHandleMax(UnsignedInteger handleMax) {
        matcher.withHandleMax(handleMax);
        return this;
    }

    public BeginExpectation withOfferedCapabilities(String... offeredCapabilities) {
        matcher.withOfferedCapabilities(offeredCapabilities);
        return this;
    }

    public BeginExpectation withOfferedCapabilities(Symbol... offeredCapabilities) {
        matcher.withOfferedCapabilities(offeredCapabilities);
        return this;
    }

    public BeginExpectation withDesiredCapabilities(String... desiredCapabilities) {
        matcher.withDesiredCapabilities(desiredCapabilities);
        return this;
    }

    public BeginExpectation withDesiredCapabilities(Symbol... desiredCapabilities) {
        matcher.withDesiredCapabilities(desiredCapabilities);
        return this;
    }

    public BeginExpectation withPropertiesMap(Map<Symbol, Object> properties) {
        matcher.withPropertiesMap(properties);
        return this;
    }

    public BeginExpectation withProperties(Map<String, Object> properties) {
        matcher.withProperties(properties);
        return this;
    }

    public BeginExpectation withProperty(String key, Object value) {
        matcher.withProperty(key, value);
        return this;
    }

    public BeginExpectation withProperty(Symbol key, Object value) {
        matcher.withProperty(key, value);
        return this;
    }

    public BeginExpectation withDesiredCapability(Symbol desiredCapability) {
        matcher.withDesiredCapability(desiredCapability);
        return this;
    }

    public BeginExpectation withDesiredCapability(String desiredCapability) {
        matcher.withDesiredCapability(desiredCapability);
        return this;
    }

    public BeginExpectation withOfferedCapability(Symbol offeredCapability) {
        matcher.withOfferedCapability(offeredCapability);
        return this;
    }

    public BeginExpectation withOfferedCapability(String offeredCapability) {
        matcher.withOfferedCapability(offeredCapability);
        return this;
    }

    //----- Matcher based with methods for more complex validation

    public BeginExpectation withRemoteChannel(Matcher<?> m) {
        matcher.addFieldMatcher(Begin.Field.REMOTE_CHANNEL, m);
        return this;
    }

    public BeginExpectation withNextOutgoingId(Matcher<?> m) {
        matcher.addFieldMatcher(Begin.Field.NEXT_OUTGOING_ID, m);
        return this;
    }

    public BeginExpectation withIncomingWindow(Matcher<?> m) {
        matcher.addFieldMatcher(Begin.Field.INCOMING_WINDOW, m);
        return this;
    }

    public BeginExpectation withOutgoingWindow(Matcher<?> m) {
        matcher.addFieldMatcher(Begin.Field.OUTGOING_WINDOW, m);
        return this;
    }

    public BeginExpectation withHandleMax(Matcher<?> m) {
        matcher.addFieldMatcher(Begin.Field.HANDLE_MAX, m);
        return this;
    }

    public BeginExpectation withOfferedCapabilities(Matcher<?> m) {
        matcher.addFieldMatcher(Begin.Field.OFFERED_CAPABILITIES, m);
        return this;
    }

    public BeginExpectation withDesiredCapabilities(Matcher<?> m) {
        matcher.addFieldMatcher(Begin.Field.DESIRED_CAPABILITIES, m);
        return this;
    }

    public BeginExpectation withProperties(Matcher<?> m) {
        matcher.addFieldMatcher(Begin.Field.PROPERTIES, m);
        return this;
    }

    @Override
    protected Matcher<ListDescribedType> getExpectationMatcher() {
        return matcher;
    }

    @Override
    protected Class<Begin> getExpectedTypeClass() {
        return Begin.class;
    }
}

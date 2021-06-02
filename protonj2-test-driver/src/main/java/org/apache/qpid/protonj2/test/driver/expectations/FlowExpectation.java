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

import org.apache.qpid.protonj2.test.driver.AMQPTestDriver;
import org.apache.qpid.protonj2.test.driver.LinkTracker;
import org.apache.qpid.protonj2.test.driver.SessionTracker;
import org.apache.qpid.protonj2.test.driver.actions.BeginInjectAction;
import org.apache.qpid.protonj2.test.driver.actions.FlowInjectAction;
import org.apache.qpid.protonj2.test.driver.codec.ListDescribedType;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Symbol;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedInteger;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedShort;
import org.apache.qpid.protonj2.test.driver.codec.transport.Flow;
import org.apache.qpid.protonj2.test.driver.matchers.transport.FlowMatcher;
import org.hamcrest.Matcher;

import io.netty.buffer.ByteBuf;

/**
 * Scripted expectation for the AMQP Flow performative
 */
public class FlowExpectation extends AbstractExpectation<Flow> {

    private final FlowMatcher matcher = new FlowMatcher();
    private FlowInjectAction response;

    public FlowExpectation(AMQPTestDriver driver) {
        super(driver);

        // Default expectations for mandatory performative fields.
        withNextIncomingId(anyOf(nullValue(), notNullValue()));
        withIncomingWindow(notNullValue());
        withNextOutgoingId(notNullValue());
        withOutgoingWindow(notNullValue());
    }

    @Override
    public FlowExpectation onChannel(int channel) {
        super.onChannel(channel);
        return this;
    }

    public FlowInjectAction respond() {
        response = new FlowInjectAction(driver);
        driver.addScriptedElement(response);
        return response;
    }

    //----- Handle the performative and configure response is told to respond

    @Override
    public void handleFlow(int frameSize, Flow flow, ByteBuf payload, int channel, AMQPTestDriver context) {
        super.handleFlow(frameSize, flow, payload, channel, context);

        final UnsignedShort remoteChannel = UnsignedShort.valueOf(channel);
        final SessionTracker session = driver.sessions().getSessionFromRemoteChannel(remoteChannel);

        if (session == null) {
            throw new AssertionError(String.format(
                "Received Flow on channel [%d] that has no matching Session for that remote channel. ", remoteChannel));
        }

        final LinkTracker linkTracker = session.handleFlow(flow);  // Can be null if Flow was session level only.

        if (response != null) {
            // Input was validated now populate response with auto values where not configured
            // to say otherwise by the test.
            if (response.onChannel() == BeginInjectAction.CHANNEL_UNSET) {
                response.onChannel(session.getLocalChannel());
            }

            // TODO: The auto response values need to be pulled from session activity to produce meaningful auto
            //       generated values for scripted responses.

            // Populate the fields of the response with defaults if non set by the test script
            if (response.getPerformative().getNextIncomingId() == null) {
                response.withNextIncomingId(flow.getNextOutgoingId().longValue()); //TODO: this could be wrong, need to know about the transfers received (and sent by peer).
            }

            if (response.getPerformative().getIncomingWindow() == null) {
                response.withIncomingWindow(Integer.MAX_VALUE); //TODO: shouldnt be hard coded
            }

            if (response.getPerformative().getNextOutgoingId() == null) {
                response.withNextOutgoingId(flow.getNextIncomingId().longValue()); //TODO: this could be wrong, need to know about the transfers sent (and received at recipient peer).
            }

            if (response.getPerformative().getOutgoingWindow() == null) {
                response.withOutgoingWindow(0); //TODO: shouldnt be hard coded, session might have senders on it as well as receivers
            }

            if (response.getPerformative().getHandle() == null && linkTracker != null) {
                response.withHandle(linkTracker.getHandle());
            }

            // TODO: blow up on response if credit not populated?

            // Other fields are left not set for now unless test script configured
        }
    }

    //----- Type specific with methods that perform simple equals checks

    public FlowExpectation withNextIncomingId(int nextIncomingId) {
        return withNextIncomingId(equalTo(UnsignedInteger.valueOf(nextIncomingId)));
    }

    public FlowExpectation withNextIncomingId(long nextIncomingId) {
        return withNextIncomingId(equalTo(UnsignedInteger.valueOf(nextIncomingId)));
    }

    public FlowExpectation withNextIncomingId(UnsignedInteger nextIncomingId) {
        return withNextIncomingId(equalTo(nextIncomingId));
    }

    public FlowExpectation withIncomingWindow(int incomingWindow) {
        return withIncomingWindow(equalTo(UnsignedInteger.valueOf(incomingWindow)));
    }

    public FlowExpectation withIncomingWindow(long incomingWindow) {
        return withIncomingWindow(equalTo(UnsignedInteger.valueOf(incomingWindow)));
    }

    public FlowExpectation withIncomingWindow(UnsignedInteger incomingWindow) {
        return withIncomingWindow(equalTo(incomingWindow));
    }

    public FlowExpectation withNextOutgoingId(int nextOutgoingId) {
        return withNextOutgoingId(equalTo(UnsignedInteger.valueOf(nextOutgoingId)));
    }

    public FlowExpectation withNextOutgoingId(long nextOutgoingId) {
        return withNextOutgoingId(equalTo(UnsignedInteger.valueOf(nextOutgoingId)));
    }

    public FlowExpectation withNextOutgoingId(UnsignedInteger nextOutgoingId) {
        return withNextOutgoingId(equalTo(nextOutgoingId));
    }

    public FlowExpectation withOutgoingWindow(int outgoingWindow) {
        return withOutgoingWindow(equalTo(UnsignedInteger.valueOf(outgoingWindow)));
    }

    public FlowExpectation withOutgoingWindow(long outgoingWindow) {
        return withOutgoingWindow(equalTo(UnsignedInteger.valueOf(outgoingWindow)));
    }

    public FlowExpectation withOutgoingWindow(UnsignedInteger outgoingWindow) {
        return withOutgoingWindow(equalTo(outgoingWindow));
    }

    public FlowExpectation withHandle(int handle) {
        return withHandle(equalTo(UnsignedInteger.valueOf(handle)));
    }

    public FlowExpectation withHandle(long handle) {
        return withHandle(equalTo(UnsignedInteger.valueOf(handle)));
    }

    public FlowExpectation withHandle(UnsignedInteger handle) {
        return withHandle(equalTo(handle));
    }

    public FlowExpectation withDeliveryCount(int deliveryCount) {
        return withDeliveryCount(equalTo(UnsignedInteger.valueOf(deliveryCount)));
    }

    public FlowExpectation withDeliveryCount(long deliveryCount) {
        return withDeliveryCount(equalTo(UnsignedInteger.valueOf(deliveryCount)));
    }

    public FlowExpectation withDeliveryCount(UnsignedInteger deliveryCount) {
        return withDeliveryCount(equalTo(deliveryCount));
    }

    public FlowExpectation withLinkCredit(int linkCredit) {
        return withLinkCredit(equalTo(UnsignedInteger.valueOf(linkCredit)));
    }

    public FlowExpectation withLinkCredit(long linkCredit) {
        return withLinkCredit(equalTo(UnsignedInteger.valueOf(linkCredit)));
    }

    public FlowExpectation withLinkCredit(UnsignedInteger linkCredit) {
        return withLinkCredit(equalTo(linkCredit));
    }

    public FlowExpectation withAvailable(int available) {
        return withAvailable(equalTo(UnsignedInteger.valueOf(available)));
    }

    public FlowExpectation withAvailable(long available) {
        return withAvailable(equalTo(UnsignedInteger.valueOf(available)));
    }

    public FlowExpectation withAvailable(UnsignedInteger available) {
        return withAvailable(equalTo(available));
    }

    public FlowExpectation withDrain(boolean drain) {
        return withDrain(equalTo(drain));
    }

    public FlowExpectation withEcho(boolean echo) {
        return withEcho(equalTo(echo));
    }

    public FlowExpectation withProperties(Map<Symbol, Object> properties) {
        return withProperties(equalTo(properties));
    }

    //----- Matcher based with methods for more complex validation

    public FlowExpectation withNextIncomingId(Matcher<?> m) {
        matcher.addFieldMatcher(Flow.Field.NEXT_INCOMING_ID, m);
        return this;
    }

    public FlowExpectation withIncomingWindow(Matcher<?> m) {
        matcher.addFieldMatcher(Flow.Field.INCOMING_WINDOW, m);
        return this;
    }

    public FlowExpectation withNextOutgoingId(Matcher<?> m) {
        matcher.addFieldMatcher(Flow.Field.NEXT_OUTGOING_ID, m);
        return this;
    }

    public FlowExpectation withOutgoingWindow(Matcher<?> m) {
        matcher.addFieldMatcher(Flow.Field.OUTGOING_WINDOW, m);
        return this;
    }

    public FlowExpectation withHandle(Matcher<?> m) {
        matcher.addFieldMatcher(Flow.Field.HANDLE, m);
        return this;
    }

    public FlowExpectation withDeliveryCount(Matcher<?> m) {
        matcher.addFieldMatcher(Flow.Field.DELIVERY_COUNT, m);
        return this;
    }

    public FlowExpectation withLinkCredit(Matcher<?> m) {
        matcher.addFieldMatcher(Flow.Field.LINK_CREDIT, m);
        return this;
    }

    public FlowExpectation withAvailable(Matcher<?> m) {
        matcher.addFieldMatcher(Flow.Field.AVAILABLE, m);
        return this;
    }

    public FlowExpectation withDrain(Matcher<?> m) {
        matcher.addFieldMatcher(Flow.Field.DRAIN, m);
        return this;
    }

    public FlowExpectation withEcho(Matcher<?> m) {
        matcher.addFieldMatcher(Flow.Field.ECHO, m);
        return this;
    }

    public FlowExpectation withProperties(Matcher<?> m) {
        matcher.addFieldMatcher(Flow.Field.PROPERTIES, m);
        return this;
    }

    @Override
    protected Matcher<ListDescribedType> getExpectationMatcher() {
        return matcher;
    }

    @Override
    protected Class<Flow> getExpectedTypeClass() {
        return Flow.class;
    }
}

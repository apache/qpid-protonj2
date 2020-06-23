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
package org.apache.qpid.proton4j.test.driver.matchers.transport;

import static org.hamcrest.CoreMatchers.equalTo;

import java.util.Map;

import org.apache.qpid.proton4j.test.driver.codec.primitives.Symbol;
import org.apache.qpid.proton4j.test.driver.codec.primitives.UnsignedInteger;
import org.apache.qpid.proton4j.test.driver.codec.transport.Flow;
import org.apache.qpid.proton4j.test.driver.matchers.ListDescribedTypeMatcher;
import org.hamcrest.Matcher;

public class FlowMatcher extends ListDescribedTypeMatcher {

    public FlowMatcher() {
        super(Flow.Field.values().length, Flow.DESCRIPTOR_CODE, Flow.DESCRIPTOR_SYMBOL);
    }

    @Override
    protected Class<?> getDescribedTypeClass() {
        return Flow.class;
    }

    //----- Type specific with methods that perform simple equals checks

    public FlowMatcher withNextIncomingId(int nextIncomingId) {
        return withNextIncomingId(equalTo(UnsignedInteger.valueOf(nextIncomingId)));
    }

    public FlowMatcher withNextIncomingId(long nextIncomingId) {
        return withNextIncomingId(equalTo(UnsignedInteger.valueOf(nextIncomingId)));
    }

    public FlowMatcher withNextIncomingId(UnsignedInteger nextIncomingId) {
        return withNextIncomingId(equalTo(nextIncomingId));
    }

    public FlowMatcher withIncomingWindow(int incomingWindow) {
        return withIncomingWindow(equalTo(UnsignedInteger.valueOf(incomingWindow)));
    }

    public FlowMatcher withIncomingWindow(long incomingWindow) {
        return withIncomingWindow(equalTo(UnsignedInteger.valueOf(incomingWindow)));
    }

    public FlowMatcher withIncomingWindow(UnsignedInteger incomingWindow) {
        return withIncomingWindow(equalTo(incomingWindow));
    }

    public FlowMatcher withNextOutgoingId(int nextOutgoingId) {
        return withNextOutgoingId(equalTo(UnsignedInteger.valueOf(nextOutgoingId)));
    }

    public FlowMatcher withNextOutgoingId(long nextOutgoingId) {
        return withNextOutgoingId(equalTo(UnsignedInteger.valueOf(nextOutgoingId)));
    }

    public FlowMatcher withNextOutgoingId(UnsignedInteger nextOutgoingId) {
        return withNextOutgoingId(equalTo(nextOutgoingId));
    }

    public FlowMatcher withOutgoingWindow(int outgoingWindow) {
        return withOutgoingWindow(equalTo(UnsignedInteger.valueOf(outgoingWindow)));
    }

    public FlowMatcher withOutgoingWindow(long outgoingWindow) {
        return withOutgoingWindow(equalTo(UnsignedInteger.valueOf(outgoingWindow)));
    }

    public FlowMatcher withOutgoingWindow(UnsignedInteger outgoingWindow) {
        return withOutgoingWindow(equalTo(outgoingWindow));
    }

    public FlowMatcher withHandle(int handle) {
        return withHandle(equalTo(UnsignedInteger.valueOf(handle)));
    }

    public FlowMatcher withHandle(long handle) {
        return withHandle(equalTo(UnsignedInteger.valueOf(handle)));
    }

    public FlowMatcher withHandle(UnsignedInteger handle) {
        return withHandle(equalTo(handle));
    }

    public FlowMatcher withDeliveryCount(int deliveryCount) {
        return withDeliveryCount(equalTo(UnsignedInteger.valueOf(deliveryCount)));
    }

    public FlowMatcher withDeliveryCount(long deliveryCount) {
        return withDeliveryCount(equalTo(UnsignedInteger.valueOf(deliveryCount)));
    }

    public FlowMatcher withDeliveryCount(UnsignedInteger deliveryCount) {
        return withDeliveryCount(equalTo(deliveryCount));
    }

    public FlowMatcher withLinkCredit(int linkCredit) {
        return withLinkCredit(equalTo(UnsignedInteger.valueOf(linkCredit)));
    }

    public FlowMatcher withLinkCredit(long linkCredit) {
        return withLinkCredit(equalTo(UnsignedInteger.valueOf(linkCredit)));
    }

    public FlowMatcher withLinkCredit(UnsignedInteger linkCredit) {
        return withLinkCredit(equalTo(linkCredit));
    }

    public FlowMatcher withAvailable(int available) {
        return withAvailable(equalTo(UnsignedInteger.valueOf(available)));
    }

    public FlowMatcher withAvailable(long available) {
        return withAvailable(equalTo(UnsignedInteger.valueOf(available)));
    }

    public FlowMatcher withAvailable(UnsignedInteger available) {
        return withAvailable(equalTo(available));
    }

    public FlowMatcher withDrain(boolean drain) {
        return withDrain(equalTo(drain));
    }

    public FlowMatcher withEcho(boolean echo) {
        return withEcho(equalTo(echo));
    }

    public FlowMatcher withProperties(Map<Symbol, Object> properties) {
        return withProperties(equalTo(properties));
    }

    //----- Matcher based with methods for more complex validation

    public FlowMatcher withNextIncomingId(Matcher<?> m) {
        addFieldMatcher(Flow.Field.NEXT_INCOMING_ID, m);
        return this;
    }

    public FlowMatcher withIncomingWindow(Matcher<?> m) {
        addFieldMatcher(Flow.Field.INCOMING_WINDOW, m);
        return this;
    }

    public FlowMatcher withNextOutgoingId(Matcher<?> m) {
        addFieldMatcher(Flow.Field.NEXT_OUTGOING_ID, m);
        return this;
    }

    public FlowMatcher withOutgoingWindow(Matcher<?> m) {
        addFieldMatcher(Flow.Field.OUTGOING_WINDOW, m);
        return this;
    }

    public FlowMatcher withHandle(Matcher<?> m) {
        addFieldMatcher(Flow.Field.HANDLE, m);
        return this;
    }

    public FlowMatcher withDeliveryCount(Matcher<?> m) {
        addFieldMatcher(Flow.Field.DELIVERY_COUNT, m);
        return this;
    }

    public FlowMatcher withLinkCredit(Matcher<?> m) {
        addFieldMatcher(Flow.Field.LINK_CREDIT, m);
        return this;
    }

    public FlowMatcher withAvailable(Matcher<?> m) {
        addFieldMatcher(Flow.Field.AVAILABLE, m);
        return this;
    }

    public FlowMatcher withDrain(Matcher<?> m) {
        addFieldMatcher(Flow.Field.DRAIN, m);
        return this;
    }

    public FlowMatcher withEcho(Matcher<?> m) {
        addFieldMatcher(Flow.Field.ECHO, m);
        return this;
    }

    public FlowMatcher withProperties(Matcher<?> m) {
        addFieldMatcher(Flow.Field.PROPERTIES, m);
        return this;
    }
}

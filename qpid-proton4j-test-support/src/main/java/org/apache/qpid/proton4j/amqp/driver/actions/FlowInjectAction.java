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

import java.util.Map;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.amqp.driver.codec.transport.Flow;

/**
 * AMQP Flow injection action which can be added to a driver for write at a specific time or
 * following on from some other action in the test script.
 */
public class FlowInjectAction extends AbstractPerformativeInjectAction<Flow> {

    private final Flow flow = new Flow();

    @Override
    public Flow getPerformative() {
        return flow;
    }

    public FlowInjectAction withNextIncomingId(long nextIncomingId) {
        flow.setNextIncomingId(UnsignedInteger.valueOf(nextIncomingId));
        return this;
    }

    public FlowInjectAction withIncomingWindow(long incomingWindow) {
        flow.setIncomingWindow(UnsignedInteger.valueOf(incomingWindow));
        return this;
    }

    public FlowInjectAction withNextOutgoingId(long nextOutgoingId) {
        flow.setNextOutgoingId(UnsignedInteger.valueOf(nextOutgoingId));
        return this;
    }

    public FlowInjectAction withOutgoingWindow(long outgoingWindow) {
        flow.setOutgoingWindow(UnsignedInteger.valueOf(outgoingWindow));
        return this;
    }

    public FlowInjectAction withHandle(long handle) {
        flow.setHandle(UnsignedInteger.valueOf(handle));
        return this;
    }

    public FlowInjectAction withDeliveryCount(long deliveryCount) {
        flow.setDeliveryCount(UnsignedInteger.valueOf(deliveryCount));
        return this;
    }

    public FlowInjectAction withLinkCredit(long linkCredit) {
        flow.setLinkCredit(UnsignedInteger.valueOf(linkCredit));
        return this;
    }

    public FlowInjectAction withAvailable(long available) {
        flow.setAvailable(UnsignedInteger.valueOf(available));
        return this;
    }

    public FlowInjectAction withDrain(boolean drain) {
        flow.setDrain(drain);
        return this;
    }

    public FlowInjectAction withEcho(boolean echo) {
        flow.setEcho(echo);
        return this;
    }

    public FlowInjectAction withProperties(Map<Symbol, Object> properties) {
        flow.setProperties(properties);
        return this;
    }

    @Override
    protected void beforeActionPerformed(AMQPTestDriver driver) {
        // We fill in a channel using the next available channel id if one isn't set, then
        // report the outbound begin to the session so it can track this new session.
        if (onChannel() == CHANNEL_UNSET) {
            onChannel(driver.getSessions().getLastOpenedSession().getLocalChannel().intValue());
        }

        // TODO - Process flow in the local side of the link when needed for added validation
    }
}

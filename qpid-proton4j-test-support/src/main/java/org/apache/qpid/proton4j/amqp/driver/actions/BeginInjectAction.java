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
import org.apache.qpid.proton4j.amqp.UnsignedShort;
import org.apache.qpid.proton4j.amqp.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.amqp.driver.codec.transport.Begin;
import org.apache.qpid.proton4j.amqp.driver.codec.util.TypeMapper;

/**
 * AMQP Begin injection action which can be added to a driver for write at a specific time or
 * following on from some other action in the test script.
 */
public class BeginInjectAction extends AbstractPerformativeInjectAction<Begin> {

    private final Begin begin = new Begin();

    /**
     * Set defaults for the required fields of the performative
     *
     * @param driver
     *      The test driver that will run this action.
     */
    public BeginInjectAction(AMQPTestDriver driver) {
        super(driver);

        begin.setNextOutgoingId(UnsignedInteger.ONE);
        begin.setIncomingWindow(UnsignedInteger.ZERO);
        begin.setOutgoingWindow(UnsignedInteger.ZERO);
    }

    @Override
    public Begin getPerformative() {
        return begin;
    }

    public BeginInjectAction withRemoteChannel(int remoteChannel) {
        begin.setRemoteChannel(UnsignedShort.valueOf((short) remoteChannel));
        return this;
    }

    public BeginInjectAction withRemoteChannel(UnsignedShort remoteChannel) {
        begin.setRemoteChannel(remoteChannel);
        return this;
    }

    public BeginInjectAction withNextOutgoingId(int nextOutgoingId) {
        begin.setNextOutgoingId(UnsignedInteger.valueOf(nextOutgoingId));
        return this;
    }

    public BeginInjectAction withNextOutgoingId(long nextOutgoingId) {
        begin.setNextOutgoingId(UnsignedInteger.valueOf(nextOutgoingId));
        return this;
    }

    public BeginInjectAction withNextOutgoingId(UnsignedInteger nextOutgoingId) {
        begin.setNextOutgoingId(nextOutgoingId);
        return this;
    }

    public BeginInjectAction withIncomingWindow(int incomingWindow) {
        begin.setIncomingWindow(UnsignedInteger.valueOf(incomingWindow));
        return this;
    }

    public BeginInjectAction withIncomingWindow(long incomingWindow) {
        begin.setIncomingWindow(UnsignedInteger.valueOf(incomingWindow));
        return this;
    }

    public BeginInjectAction withIncomingWindow(UnsignedInteger incomingWindow) {
        begin.setIncomingWindow(incomingWindow);
        return this;
    }

    public BeginInjectAction withOutgoingWindow(int outgoingWindow) {
        begin.setOutgoingWindow(UnsignedInteger.valueOf(outgoingWindow));
        return this;
    }

    public BeginInjectAction withOutgoingWindow(long outgoingWindow) {
        begin.setOutgoingWindow(UnsignedInteger.valueOf(outgoingWindow));
        return this;
    }

    public BeginInjectAction withOutgoingWindow(UnsignedInteger outgoingWindow) {
        begin.setOutgoingWindow(outgoingWindow);
        return this;
    }

    public BeginInjectAction withHandleMax(int handleMax) {
        begin.setHandleMax(UnsignedInteger.valueOf(handleMax));
        return this;
    }

    public BeginInjectAction withHandleMax(long handleMax) {
        begin.setHandleMax(UnsignedInteger.valueOf(handleMax));
        return this;
    }

    public BeginInjectAction withHandleMax(UnsignedInteger handleMax) {
        begin.setHandleMax(handleMax);
        return this;
    }

    public BeginInjectAction withOfferedCapabilities(String... offeredCapabilities) {
        begin.setOfferedCapabilities(TypeMapper.toSymbolArray(offeredCapabilities));
        return this;
    }

    public BeginInjectAction withOfferedCapabilities(Symbol... offeredCapabilities) {
        begin.setOfferedCapabilities(offeredCapabilities);
        return this;
    }

    public BeginInjectAction withDesiredCapabilities(String... desiredCapabilities) {
        begin.setDesiredCapabilities(TypeMapper.toSymbolArray(desiredCapabilities));
        return this;
    }

    public BeginInjectAction withDesiredCapabilities(Symbol... desiredCapabilities) {
        begin.setDesiredCapabilities(desiredCapabilities);
        return this;
    }

    public BeginInjectAction withPropertiesMap(Map<String, Object> properties) {
        begin.setProperties(TypeMapper.toSymbolKeyedMap(properties));
        return this;
    }

    public BeginInjectAction withProperties(Map<Symbol, Object> properties) {
        begin.setProperties(properties);
        return this;
    }

    @Override
    protected void beforeActionPerformed(AMQPTestDriver driver) {
        // We fill in a channel using the next available channel id if one isn't set, then
        // report the outbound begin to the session so it can track this new session.
        if (onChannel() == CHANNEL_UNSET) {
            onChannel(driver.getSessions().getNextChannelId());
        }

        driver.getSessions().processLocalBegin(begin, onChannel());
    }
}

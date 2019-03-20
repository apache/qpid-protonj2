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
import java.util.function.Consumer;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.amqp.driver.ScriptedAction;
import org.apache.qpid.proton4j.amqp.transport.Begin;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;

/**
 * AMQP Begin injection action which can be added to a driver for write at a specific time or
 * following on from some other action in the test script.
 */
public class BeginInjectAction implements ScriptedAction {

    public static final int CHANNEL_UNSET = -1;

    private final Begin begin;
    private int channel = CHANNEL_UNSET;

    public BeginInjectAction(Begin begin, int channel) {
        this.begin = begin;
        this.channel = channel;
    }

    public BeginInjectAction onChannel(int channel) {
        this.channel = channel;
        return this;
    }

    public int onChannel() {
        return this.channel;
    }

    public Begin getBegin() {
        return begin;
    }

    public BeginInjectAction withRemoteChannel(int remoteChannel) {
        begin.setRemoteChannel(remoteChannel);
        return this;
    }

    public BeginInjectAction withNextOutgoingId(long nextOutgoingId) {
        begin.setNextOutgoingId(nextOutgoingId);
        return this;
    }

    public BeginInjectAction withIncomingWindow(long incomingWindow) {
        begin.setIncomingWindow(incomingWindow);
        return this;
    }

    public BeginInjectAction withOutgoingWindow(long outgoingWindow) {
        begin.setOutgoingWindow(outgoingWindow);
        return this;
    }

    public BeginInjectAction withHandleMax(long handleMax) {
        begin.setHandleMax(handleMax);
        return this;
    }

    public BeginInjectAction withOfferedCapabilities(Symbol... offeredCapabilities) {
        begin.setOfferedCapabilities(offeredCapabilities);
        return this;
    }

    public BeginInjectAction withDesiredCapabilities(Symbol... desiredCapabilities) {
        begin.setDesiredCapabilities(desiredCapabilities);
        return this;
    }

    public BeginInjectAction withProperties(Map<Symbol, Object> properties) {
        begin.setProperties(properties);
        return this;
    }

    @Override
    public void perform(AMQPTestDriver driver, Consumer<ProtonBuffer> consumer) {
        driver.sendAMQPFrame(channel, begin, null);
    }
}

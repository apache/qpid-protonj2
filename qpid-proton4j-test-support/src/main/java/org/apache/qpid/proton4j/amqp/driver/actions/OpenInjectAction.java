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
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.UnsignedShort;
import org.apache.qpid.proton4j.amqp.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.amqp.driver.ScriptedAction;
import org.apache.qpid.proton4j.amqp.transport.Open;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;

/**
 * AMQP Open injection action which can be added to a driver for write at a specific time or
 * following on from some other action in the test script.
 */
public class OpenInjectAction implements ScriptedAction {

    private final Open open;
    private int channel;

    public OpenInjectAction(Open open, int channel) {
        this.open = open;
        this.channel = channel;
    }

    public OpenInjectAction onChannel(int channel) {
        this.channel = channel;
        return this;
    }

    public OpenInjectAction withContainerId(String containerId) {
        open.setContainerId(containerId);
        return this;
    }

    public OpenInjectAction withHostname(String hostname) {
        open.setHostname(hostname);
        return this;
    }

    public OpenInjectAction withMaxFrameSize(UnsignedInteger maxFrameSize) {
        open.setMaxFrameSize(maxFrameSize);
        return this;
    }

    public OpenInjectAction withChannelMax(UnsignedShort channelMax) {
        open.setChannelMax(channelMax);
        return this;
    }

    public OpenInjectAction withIdleTimeOut(UnsignedInteger idleTimeout) {
        open.setIdleTimeOut(idleTimeout);
        return this;
    }

    public OpenInjectAction withOutgoingLocales(Symbol... outgoingLocales) {
        open.setOutgoingLocales(outgoingLocales);
        return this;
    }

    public OpenInjectAction withIncomingLocales(Symbol... incomingLocales) {
        open.setIncomingLocales(incomingLocales);
        return this;
    }

    public OpenInjectAction withOfferedCapabilities(Symbol... offeredCapabilities) {
        open.setOfferedCapabilities(offeredCapabilities);
        return this;
    }

    public OpenInjectAction withDesiredCapabilities(Symbol... desiredCapabilities) {
        open.setDesiredCapabilities(desiredCapabilities);
        return this;
    }

    public OpenInjectAction withProperties(Map<Symbol, Object> properties) {
        open.setProperties(properties);
        return this;
    }

    @Override
    public void perform(AMQPTestDriver driver, Consumer<ProtonBuffer> consumer) {
        driver.sendAMQPFrame(channel, open, null);
    }
}

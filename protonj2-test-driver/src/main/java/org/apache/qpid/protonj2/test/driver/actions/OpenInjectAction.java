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
package org.apache.qpid.protonj2.test.driver.actions;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.protonj2.test.driver.AMQPTestDriver;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Symbol;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedInteger;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedShort;
import org.apache.qpid.protonj2.test.driver.codec.transport.Open;
import org.apache.qpid.protonj2.test.driver.codec.util.TypeMapper;

/**
 * AMQP Open injection action which can be added to a driver for write at a specific time or
 * following on from some other action in the test script.
 */
public class OpenInjectAction extends AbstractPerformativeInjectAction<Open> {

    private final Open open = new Open();

    public OpenInjectAction(AMQPTestDriver driver) {
        super(driver);
    }

    @Override
    public Open getPerformative() {
        return open;
    }

    @Override
    protected void beforeActionPerformed(AMQPTestDriver driver) {
        if (getPerformative().getContainerId() == null) {
            getPerformative().setContainerId("driver");
        }

        if (onChannel() == CHANNEL_UNSET) {
            onChannel(0);
        }
    }

    public OpenInjectAction withContainerId(String containerId) {
        open.setContainerId(containerId);
        return this;
    }

    public OpenInjectAction withHostname(String hostname) {
        open.setHostname(hostname);
        return this;
    }

    public OpenInjectAction withMaxFrameSize(int maxFrameSize) {
        open.setMaxFrameSize(UnsignedInteger.valueOf(maxFrameSize));
        return this;
    }

    public OpenInjectAction withMaxFrameSize(long maxFrameSize) {
        open.setMaxFrameSize(UnsignedInteger.valueOf(maxFrameSize));
        return this;
    }

    public OpenInjectAction withMaxFrameSize(UnsignedInteger maxFrameSize) {
        open.setMaxFrameSize(maxFrameSize);
        return this;
    }

    public OpenInjectAction withChannelMax(int channelMax) {
        open.setChannelMax(UnsignedShort.valueOf(channelMax));
        return this;
    }

    public OpenInjectAction withChannelMax(short channelMax) {
        open.setChannelMax(UnsignedShort.valueOf(channelMax));
        return this;
    }

    public OpenInjectAction withChannelMax(UnsignedShort channelMax) {
        open.setChannelMax(channelMax);
        return this;
    }

    public OpenInjectAction withIdleTimeOut(int idleTimeout) {
        open.setIdleTimeOut(UnsignedInteger.valueOf(idleTimeout));
        return this;
    }

    public OpenInjectAction withIdleTimeOut(long idleTimeout) {
        open.setIdleTimeOut(UnsignedInteger.valueOf(idleTimeout));
        return this;
    }

    public OpenInjectAction withIdleTimeOut(UnsignedInteger idleTimeout) {
        open.setIdleTimeOut(idleTimeout);
        return this;
    }

    public OpenInjectAction withOutgoingLocales(String... outgoingLocales) {
        open.setOutgoingLocales(TypeMapper.toSymbolArray(outgoingLocales));
        return this;
    }

    public OpenInjectAction withOutgoingLocales(Symbol... outgoingLocales) {
        open.setOutgoingLocales(outgoingLocales);
        return this;
    }

    public OpenInjectAction withIncomingLocales(String... incomingLocales) {
        open.setIncomingLocales(TypeMapper.toSymbolArray(incomingLocales));
        return this;
    }

    public OpenInjectAction withIncomingLocales(Symbol... incomingLocales) {
        open.setIncomingLocales(incomingLocales);
        return this;
    }

    public OpenInjectAction withOfferedCapabilities(String... offeredCapabilities) {
        open.setOfferedCapabilities(TypeMapper.toSymbolArray(offeredCapabilities));
        return this;
    }

    public OpenInjectAction withOfferedCapabilities(Symbol... offeredCapabilities) {
        open.setOfferedCapabilities(offeredCapabilities);
        return this;
    }

    public OpenInjectAction withDesiredCapabilities(String... desiredCapabilities) {
        open.setDesiredCapabilities(TypeMapper.toSymbolArray(desiredCapabilities));
        return this;
    }

    public OpenInjectAction withDesiredCapabilities(Symbol... desiredCapabilities) {
        open.setDesiredCapabilities(desiredCapabilities);
        return this;
    }

    public OpenInjectAction withProperties(Map<String, Object> properties) {
        open.setProperties(TypeMapper.toSymbolKeyedMap(properties));
        return this;
    }

    public OpenInjectAction withPropertiesMap(Map<Symbol, Object> properties) {
        open.setProperties(properties);
        return this;
    }

    public OpenInjectAction withProperty(Symbol key, Object value) {
        if (open.getProperties() == null) {
            open.setProperties(new LinkedHashMap<>());
        }

        open.getProperties().put(key, value);
        return this;
    }

    public OpenInjectAction withProperty(String key, Object value) {
        return withProperty(Symbol.valueOf(key), value);
    }
}

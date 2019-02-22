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
package org.apache.qpid.proton4j.amqp.transport;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.UnsignedShort;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;

public final class Open implements Performative {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000010L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:open:list");

    private String containerId = "";
    private String hostname;
    private UnsignedInteger maxFrameSize = UnsignedInteger.valueOf(0xffffffff);
    private UnsignedShort channelMax = UnsignedShort.valueOf((short) 65535);
    private UnsignedInteger idleTimeOut;
    private Symbol[] outgoingLocales;
    private Symbol[] incomingLocales;
    private Symbol[] offeredCapabilities;
    private Symbol[] desiredCapabilities;
    private Map<Symbol, Object> properties;

    @Override
    public Open copy() {
        Open copy = new Open();

        copy.setContainerId(containerId);
        copy.setHostname(hostname);
        copy.setMaxFrameSize(maxFrameSize);
        copy.setChannelMax(channelMax);
        copy.setIdleTimeOut(idleTimeOut);
        if (outgoingLocales != null) {
            copy.setOutgoingLocales(Arrays.copyOf(outgoingLocales, outgoingLocales.length));
        }
        if (incomingLocales != null) {
            copy.setIncomingLocales(Arrays.copyOf(incomingLocales, incomingLocales.length));
        }
        if (offeredCapabilities != null) {
            copy.setOfferedCapabilities(Arrays.copyOf(offeredCapabilities, offeredCapabilities.length));
        }
        if (desiredCapabilities != null) {
            copy.setOfferedCapabilities(Arrays.copyOf(desiredCapabilities, desiredCapabilities.length));
        }
        if (properties != null) {
            copy.setProperties(new LinkedHashMap<>(properties));
        }

        return copy;
    }

    public String getContainerId() {
        return containerId;
    }

    public Open setContainerId(String containerId) {
        if (containerId == null) {
            throw new NullPointerException("the container-id field is mandatory");
        }

        this.containerId = containerId;
        return this;
    }

    public String getHostname() {
        return hostname;
    }

    public Open setHostname(String hostname) {
        this.hostname = hostname;
        return this;
    }

    public UnsignedInteger getMaxFrameSize() {
        return maxFrameSize;
    }

    public Open setMaxFrameSize(UnsignedInteger maxFrameSize) {
        this.maxFrameSize = maxFrameSize;
        return this;
    }

    public UnsignedShort getChannelMax() {
        return channelMax;
    }

    public Open setChannelMax(UnsignedShort channelMax) {
        this.channelMax = channelMax;
        return this;
    }

    public UnsignedInteger getIdleTimeOut() {
        return idleTimeOut;
    }

    public Open setIdleTimeOut(UnsignedInteger idleTimeOut) {
        this.idleTimeOut = idleTimeOut;
        return this;
    }

    public Symbol[] getOutgoingLocales() {
        return outgoingLocales;
    }

    public Open setOutgoingLocales(Symbol... outgoingLocales) {
        this.outgoingLocales = outgoingLocales;
        return this;
    }

    public Symbol[] getIncomingLocales() {
        return incomingLocales;
    }

    public Open setIncomingLocales(Symbol... incomingLocales) {
        this.incomingLocales = incomingLocales;
        return this;
    }

    public Symbol[] getOfferedCapabilities() {
        return offeredCapabilities;
    }

    public Open setOfferedCapabilities(Symbol... offeredCapabilities) {
        this.offeredCapabilities = offeredCapabilities;
        return this;
    }

    public Symbol[] getDesiredCapabilities() {
        return desiredCapabilities;
    }

    public Open setDesiredCapabilities(Symbol... desiredCapabilities) {
        this.desiredCapabilities = desiredCapabilities;
        return this;
    }

    public Map<Symbol, Object> getProperties() {
        return properties;
    }

    public Open setProperties(Map<Symbol, Object> properties) {
        this.properties = properties;
        return this;
    }

    @Override
    public PerformativeType getPerformativeType() {
        return PerformativeType.OPEN;
    }

    @Override
    public <E> void invoke(PerformativeHandler<E> handler, ProtonBuffer payload, int channel, E context) {
        handler.handleOpen(this, payload, channel, context);
    }

    @Override
    public String toString() {
        return "Open{" +
               " containerId='" + containerId + '\'' +
               ", hostname='" + hostname + '\'' +
               ", maxFrameSize=" + maxFrameSize +
               ", channelMax=" + channelMax +
               ", idleTimeOut=" + idleTimeOut +
               ", outgoingLocales=" + (outgoingLocales == null ? null : Arrays.asList(outgoingLocales)) +
               ", incomingLocales=" + (incomingLocales == null ? null : Arrays.asList(incomingLocales)) +
               ", offeredCapabilities=" + (offeredCapabilities == null ? null : Arrays.asList(offeredCapabilities)) +
               ", desiredCapabilities=" + (desiredCapabilities == null ? null : Arrays.asList(desiredCapabilities)) +
               ", properties=" + properties +
               '}';
    }
}

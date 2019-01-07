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

public final class Begin implements Performative {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000011L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:begin:list");

    private UnsignedShort remoteChannel;
    private UnsignedInteger nextOutgoingId;
    private UnsignedInteger incomingWindow;
    private UnsignedInteger outgoingWindow;
    private UnsignedInteger handleMax = UnsignedInteger.valueOf(0xffffffff);
    private Symbol[] offeredCapabilities;
    private Symbol[] desiredCapabilities;
    private Map<Object, Object> properties;

    @Override
    public PerformativeType getPerformativeType() {
        return PerformativeType.BEGIN;
    }

    @Override
    public Begin copy() {
        Begin copy = new Begin();

        copy.setRemoteChannel(remoteChannel);
        copy.setNextOutgoingId(nextOutgoingId);
        copy.setIncomingWindow(incomingWindow);
        copy.setOutgoingWindow(outgoingWindow);
        copy.setHandleMax(handleMax);
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

    public UnsignedShort getRemoteChannel() {
        return remoteChannel;
    }

    public void setRemoteChannel(UnsignedShort remoteChannel) {
        this.remoteChannel = remoteChannel;
    }

    public UnsignedInteger getNextOutgoingId() {
        return nextOutgoingId;
    }

    public void setNextOutgoingId(UnsignedInteger nextOutgoingId) {
        if (nextOutgoingId == null) {
            throw new NullPointerException("the next-outgoing-id field is mandatory");
        }

        this.nextOutgoingId = nextOutgoingId;
    }

    public UnsignedInteger getIncomingWindow() {
        return incomingWindow;
    }

    public void setIncomingWindow(UnsignedInteger incomingWindow) {
        if (incomingWindow == null) {
            throw new NullPointerException("the incoming-window field is mandatory");
        }

        this.incomingWindow = incomingWindow;
    }

    public UnsignedInteger getOutgoingWindow() {
        return outgoingWindow;
    }

    public void setOutgoingWindow(UnsignedInteger outgoingWindow) {
        if (outgoingWindow == null) {
            throw new NullPointerException("the outgoing-window field is mandatory");
        }

        this.outgoingWindow = outgoingWindow;
    }

    public UnsignedInteger getHandleMax() {
        return handleMax;
    }

    public void setHandleMax(UnsignedInteger handleMax) {
        this.handleMax = handleMax;
    }

    public Symbol[] getOfferedCapabilities() {
        return offeredCapabilities;
    }

    public void setOfferedCapabilities(Symbol... offeredCapabilities) {
        this.offeredCapabilities = offeredCapabilities;
    }

    public Symbol[] getDesiredCapabilities() {
        return desiredCapabilities;
    }

    public void setDesiredCapabilities(Symbol... desiredCapabilities) {
        this.desiredCapabilities = desiredCapabilities;
    }

    public Map<Object, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<Object, Object> properties) {
        this.properties = properties;
    }

    @Override
    public <E> void invoke(PerformativeHandler<E> handler, ProtonBuffer payload, E context) {
        handler.handleBegin(this, payload, context);
    }

    @Override
    public String toString() {
        return "Begin{" +
               "remoteChannel=" + remoteChannel +
               ", nextOutgoingId=" + nextOutgoingId +
               ", incomingWindow=" + incomingWindow +
               ", outgoingWindow=" + outgoingWindow +
               ", handleMax=" + handleMax +
               ", offeredCapabilities=" + (offeredCapabilities == null ? null : Arrays.asList(offeredCapabilities)) +
               ", desiredCapabilities=" + (desiredCapabilities == null ? null : Arrays.asList(desiredCapabilities)) +
               ", properties=" + properties +
               '}';
    }
}

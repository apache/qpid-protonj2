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
package org.apache.qpid.protonj2.types.transport;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.UnsignedShort;

public final class Begin implements Performative {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000011L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:begin:list");

    private static final long UINT_MAX = 0xFFFFFFFFL;

    private static final int REMOTE_CHANNEL = 1;
    private static final int NEXT_OUTGOING_ID = 2;
    private static final int INCOMING_WINDOW = 4;
    private static final int OUTGOING_WINDOW = 8;
    private static final int HANDLE_MAX = 16;
    private static final int OFFERED_CAPABILITIES = 32;
    private static final int DESIRED_CAPABILITIES = 64;
    private static final int PROPERTIES = 128;

    private int modified = 0;

    // TODO - Consider using the matching signed types instead of next largest
    //        for these values as in most cases we don't actually care about sign.
    //        In the cases we do care we could just do the math and make these
    //        interfaces simpler and not check all over the place for overflow.

    private int remoteChannel;
    private long nextOutgoingId;
    private long incomingWindow;
    private long outgoingWindow;
    private long handleMax = UnsignedInteger.MAX_VALUE.longValue();
    private Symbol[] offeredCapabilities;
    private Symbol[] desiredCapabilities;
    private Map<Symbol, Object> properties;

    @Override
    public PerformativeType getPerformativeType() {
        return PerformativeType.BEGIN;
    }

    @Override
    public Begin copy() {
        Begin copy = new Begin();

        copy.remoteChannel = remoteChannel;
        copy.nextOutgoingId = nextOutgoingId;
        copy.incomingWindow = incomingWindow;
        copy.outgoingWindow = outgoingWindow;
        copy.handleMax = handleMax;
        if (offeredCapabilities != null) {
            copy.offeredCapabilities = Arrays.copyOf(offeredCapabilities, offeredCapabilities.length);
        }
        if (desiredCapabilities != null) {
            copy.desiredCapabilities = Arrays.copyOf(desiredCapabilities, desiredCapabilities.length);
        }
        if (properties != null) {
            copy.properties = new LinkedHashMap<>(properties);
        }
        copy.modified = modified;

        return copy;
    }

    //----- Query the state of the Header object -----------------------------//

    public boolean isEmpty() {
        return modified == 0;
    }

    public int getElementCount() {
        return 32 - Integer.numberOfLeadingZeros(modified);
    }

    public boolean hasElement(int index) {
        final int value = 1 << index;
        return (modified & value) == value;
    }

    public boolean hasRemoteChannel() {
        return (modified & REMOTE_CHANNEL) == REMOTE_CHANNEL;
    }

    public boolean hasNextOutgoingId() {
        return (modified & NEXT_OUTGOING_ID) == NEXT_OUTGOING_ID;
    }

    public boolean hasIncomingWindow() {
        return (modified & INCOMING_WINDOW) == INCOMING_WINDOW;
    }

    public boolean hasOutgoingWindow() {
        return (modified & OUTGOING_WINDOW) == OUTGOING_WINDOW;
    }

    public boolean hasHandleMax() {
        return (modified & HANDLE_MAX) == HANDLE_MAX;
    }

    public boolean hasOfferedCapabilities() {
        return (modified & OFFERED_CAPABILITIES) == OFFERED_CAPABILITIES;
    }

    public boolean hasDesiredCapabilities() {
        return (modified & DESIRED_CAPABILITIES) == DESIRED_CAPABILITIES;
    }

    public boolean hasProperties() {
        return (modified & PROPERTIES) == PROPERTIES;
    }

    //----- Access to the member data with state checks

    public int getRemoteChannel() {
        return remoteChannel;
    }

    public Begin setRemoteChannel(int remoteChannel) {
        if (remoteChannel < 0 || remoteChannel > UnsignedShort.MAX_VALUE.intValue()) {
            throw new IllegalArgumentException("Remote channel value given is out of range: " + remoteChannel);
        } else {
            modified |= REMOTE_CHANNEL;
        }

        this.remoteChannel = remoteChannel;
        return this;
    }

    public long getNextOutgoingId() {
        return nextOutgoingId;
    }

    public Begin setNextOutgoingId(int nextOutgoingId) {
        modified |= NEXT_OUTGOING_ID;
        this.nextOutgoingId = Integer.toUnsignedLong(nextOutgoingId);
        return this;
    }

    public Begin setNextOutgoingId(long nextOutgoingId) {
        if (nextOutgoingId < 0 || nextOutgoingId > UINT_MAX) {
            throw new IllegalArgumentException("Next Outgoing Id value given is out of range: " + nextOutgoingId);
        } else {
            modified |= NEXT_OUTGOING_ID;
        }

        this.nextOutgoingId = nextOutgoingId;
        return this;
    }

    public long getIncomingWindow() {
        return incomingWindow;
    }

    public Begin setIncomingWindow(int incomingWindow) {
        modified |= INCOMING_WINDOW;
        this.incomingWindow = Integer.toUnsignedLong(incomingWindow);
        return this;
    }

    public Begin setIncomingWindow(long incomingWindow) {
        if (incomingWindow < 0 || incomingWindow > UINT_MAX) {
            throw new IllegalArgumentException("Incoming Window value given is out of range: " + incomingWindow);
        } else {
            modified |= INCOMING_WINDOW;
        }

        this.incomingWindow = incomingWindow;
        return this;
    }

    public long getOutgoingWindow() {
        return outgoingWindow;
    }

    public Begin setOutgoingWindow(int outgoingWindow) {
        modified |= OUTGOING_WINDOW;
        this.outgoingWindow = Integer.toUnsignedLong(outgoingWindow);
        return this;
    }

    public Begin setOutgoingWindow(long outgoingWindow) {
        if (outgoingWindow < 0 || outgoingWindow > UINT_MAX) {
            throw new IllegalArgumentException("Incoming Window value given is out of range: " + outgoingWindow);
        } else {
            modified |= OUTGOING_WINDOW;
        }

        this.outgoingWindow = outgoingWindow;
        return this;
    }

    public long getHandleMax() {
        return handleMax;
    }

    public Begin setHandleMax(int handleMax) {
        modified |= HANDLE_MAX;
        this.handleMax = Integer.toUnsignedLong(handleMax);
        return this;
    }

    public Begin setHandleMax(long handleMax) {
        if (handleMax < 0 || handleMax > UINT_MAX) {
            throw new IllegalArgumentException("Handle Max value given is out of range: " + handleMax);
        } else {
            modified |= HANDLE_MAX;
        }

        this.handleMax = handleMax;
        return this;
    }

    public Symbol[] getOfferedCapabilities() {
        return offeredCapabilities;
    }

    public Begin setOfferedCapabilities(Symbol... offeredCapabilities) {
        if (offeredCapabilities != null) {
            modified |= OFFERED_CAPABILITIES;
        } else {
            modified &= ~OFFERED_CAPABILITIES;
        }

        this.offeredCapabilities = offeredCapabilities;
        return this;
    }

    public Symbol[] getDesiredCapabilities() {
        return desiredCapabilities;
    }

    public Begin setDesiredCapabilities(Symbol... desiredCapabilities) {
        if (desiredCapabilities != null) {
            modified |= DESIRED_CAPABILITIES;
        } else {
            modified &= ~DESIRED_CAPABILITIES;
        }

        this.desiredCapabilities = desiredCapabilities;
        return this;
    }

    public Map<Symbol, Object> getProperties() {
        return properties;
    }

    public Begin setProperties(Map<Symbol, Object> properties) {
        if (properties != null) {
            modified |= PROPERTIES;
        } else {
            modified &= ~PROPERTIES;
        }

        this.properties = properties;
        return this;
    }

    @Override
    public <E> void invoke(PerformativeHandler<E> handler, ProtonBuffer payload, int channel, E context) {
        handler.handleBegin(this, payload, channel, context);
    }

    @Override
    public String toString() {
        return "Begin{" +
               "remoteChannel=" + (hasRemoteChannel() ? remoteChannel : "null") +
               ", nextOutgoingId=" + (hasNextOutgoingId() ? nextOutgoingId : "null") +
               ", incomingWindow=" + (hasIncomingWindow() ? incomingWindow : "null") +
               ", outgoingWindow=" + (hasOutgoingWindow() ? outgoingWindow : "null") +
               ", handleMax=" + (hasHandleMax() ? handleMax : "null") +
               ", offeredCapabilities=" + (offeredCapabilities == null ? "null" : Arrays.asList(offeredCapabilities)) +
               ", desiredCapabilities=" + (desiredCapabilities == null ? "null" : Arrays.asList(desiredCapabilities)) +
               ", properties=" + properties +
               '}';
    }
}

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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;

public final class Flow implements Performative {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000013L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:flow:list");

    private static final long UINT_MAX = 0xFFFFFFFFL;

    private static final int NEXT_INCOMING_ID = 1;
    private static final int INCOMING_WINDOW = 2;
    private static final int NEXT_OUTGOING_ID = 4;
    private static final int OUTGOING_WINDOW = 8;
    private static final int HANDLE = 16;
    private static final int DELIVERY_COUNT = 32;
    private static final int LINK_CREDIT = 64;
    private static final int AVAILABLE = 128;
    private static final int DRAIN = 256;
    private static final int ECHO = 512;
    private static final int PROPERTIES = 1024;

    private int modified = 0;

    // TODO - Consider using the matching signed types instead of next largest
    //        for these values as in most cases we don't actually care about sign.
    //        In the cases we do care we could just do the math and make these
    //        interfaces simpler and not check all over the place for overflow.

    private long nextIncomingId;
    private long incomingWindow;
    private long nextOutgoingId;
    private long outgoingWindow;
    private long handle;
    private long deliveryCount;
    private long linkCredit;
    private long available;
    private boolean drain;
    private boolean echo;
    private Map<Symbol, Object> properties;

    //----- Query the state of the Flow object -----------------------------//

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

    public boolean hasNextIncomingId() {
        return (modified & NEXT_INCOMING_ID) == NEXT_INCOMING_ID;
    }

    public boolean hasIncomingWindow() {
        return (modified & INCOMING_WINDOW) == INCOMING_WINDOW;
    }

    public boolean hasNextOutgoingId() {
        return (modified & NEXT_OUTGOING_ID) == NEXT_OUTGOING_ID;
    }

    public boolean hasOutgoingWindow() {
        return (modified & OUTGOING_WINDOW) == OUTGOING_WINDOW;
    }

    public boolean hasHandle() {
        return (modified & HANDLE) == HANDLE;
    }

    public boolean hasDeliveryCount() {
        return (modified & DELIVERY_COUNT) == DELIVERY_COUNT;
    }

    public boolean hasLinkCredit() {
        return (modified & LINK_CREDIT) == LINK_CREDIT;
    }

    public boolean hasAvailable() {
        return (modified & AVAILABLE) == AVAILABLE;
    }

    public boolean hasDrain() {
        return (modified & DRAIN) == DRAIN;
    }

    public boolean hasEcho() {
        return (modified & ECHO) == ECHO;
    }

    public boolean hasProperties() {
        return (modified & PROPERTIES) == PROPERTIES;
    }

    //----- Access the AMQP Flow object ------------------------------------//

    public Flow reset() {
        modified = 0;
        nextIncomingId = 0;
        incomingWindow = 0;
        nextOutgoingId = 0;
        outgoingWindow = 0;
        handle = 0;
        deliveryCount = 0;
        linkCredit = 0;
        available = 0;
        drain = false;
        echo = false;
        properties = null;

        return this;
    }

    public long getNextIncomingId() {
        return nextIncomingId;
    }

    public Flow setNextIncomingId(int nextIncomingId) {
        modified |= NEXT_INCOMING_ID;
        this.nextIncomingId = Integer.toUnsignedLong(nextIncomingId);
        return this;
    }

    public Flow setNextIncomingId(long nextIncomingId) {
        if (nextIncomingId < 0 || nextIncomingId > UINT_MAX) {
            throw new IllegalArgumentException("Next Incoming Id value given is out of range: " + nextIncomingId);
        } else {
            modified |= NEXT_INCOMING_ID;
        }

        this.nextIncomingId = nextIncomingId;
        return this;
    }

    public Flow clearNextIncomingId() {
        modified &= ~NEXT_INCOMING_ID;
        nextIncomingId = 0;
        return this;
    }

    public long getIncomingWindow() {
        return incomingWindow;
    }

    public Flow setIncomingWindow(int incomingWindow) {
        modified |= INCOMING_WINDOW;
        this.incomingWindow = Integer.toUnsignedLong(incomingWindow);
        return this;
    }

    public Flow setIncomingWindow(long incomingWindow) {
        if (incomingWindow < 0 || incomingWindow > UINT_MAX) {
            throw new IllegalArgumentException("Incoming Window value given is out of range: " + incomingWindow);
        } else {
            modified |= INCOMING_WINDOW;
        }

        this.incomingWindow = incomingWindow;
        return this;
    }

    public Flow clearIncomingWindow() {
        modified &= ~INCOMING_WINDOW;
        incomingWindow = 0;
        return this;
    }

    public long getNextOutgoingId() {
        return nextOutgoingId;
    }

    public Flow setNextOutgoingId(int nextOutgoingId) {
        modified |= NEXT_OUTGOING_ID;
        this.nextOutgoingId = Integer.toUnsignedLong(nextOutgoingId);
        return this;
    }

    public Flow setNextOutgoingId(long nextOutgoingId) {
        if (nextOutgoingId < 0 || nextOutgoingId > UINT_MAX) {
            throw new IllegalArgumentException("Next Outgoing Id value given is out of range: " + nextOutgoingId);
        } else {
            modified |= NEXT_OUTGOING_ID;
        }

        this.nextOutgoingId = nextOutgoingId;
        return this;
    }

    public Flow clearNextOutgoingId() {
        modified &= ~NEXT_OUTGOING_ID;
        nextOutgoingId = 0;
        return this;
    }

    public long getOutgoingWindow() {
        return outgoingWindow;
    }

    public Flow setOutgoingWindow(int outgoingWindow) {
        modified |= OUTGOING_WINDOW;
        this.outgoingWindow = Integer.toUnsignedLong(outgoingWindow);
        return this;
    }

    public Flow setOutgoingWindow(long outgoingWindow) {
        if (outgoingWindow < 0 || outgoingWindow > UINT_MAX) {
            throw new IllegalArgumentException("Outgoing Window value given is out of range: " + outgoingWindow);
        } else {
            modified |= OUTGOING_WINDOW;
        }

        this.outgoingWindow = outgoingWindow;
        return this;
    }

    public Flow clearOutgoingWindow() {
        modified &= ~OUTGOING_WINDOW;
        outgoingWindow = 0;
        return this;
    }

    public long getHandle() {
        return handle;
    }

    public Flow setHandle(int handle) {
        modified |= HANDLE;
        this.handle = Integer.toUnsignedLong(handle);
        return this;
    }

    public Flow setHandle(long handle) {
        if (handle < 0 || handle > UINT_MAX) {
            throw new IllegalArgumentException("Handle value given is out of range: " + handle);
        } else {
            modified |= HANDLE;
        }

        this.handle = handle;
        return this;
    }

    public Flow clearHandle() {
        modified &= ~HANDLE;
        handle = 0;
        return this;
    }

    public long getDeliveryCount() {
        return deliveryCount;
    }

    public Flow setDeliveryCount(int deliveryCount) {
        modified |= DELIVERY_COUNT;
        this.deliveryCount = Integer.toUnsignedLong(deliveryCount);
        return this;
    }

    public Flow setDeliveryCount(long deliveryCount) {
        if (deliveryCount < 0 || deliveryCount > UINT_MAX) {
            throw new IllegalArgumentException("Delivery Count value given is out of range: " + deliveryCount);
        } else {
            modified |= DELIVERY_COUNT;
        }

        this.deliveryCount = deliveryCount;
        return this;
    }

    public Flow clearDeliveryCount() {
        modified &= ~DELIVERY_COUNT;
        deliveryCount = 0;
        return this;
    }

    public long getLinkCredit() {
        return linkCredit;
    }

    public Flow setLinkCredit(int linkCredit) {
        modified |= LINK_CREDIT;
        this.linkCredit = Integer.toUnsignedLong(linkCredit);
        return this;
    }

    public Flow setLinkCredit(long linkCredit) {
        if (linkCredit < 0 || linkCredit > UINT_MAX) {
            throw new IllegalArgumentException("Link Credit value given is out of range: " + linkCredit);
        } else {
            modified |= LINK_CREDIT;
        }

        this.linkCredit = linkCredit;
        return this;
    }

    public Flow clearLinkCredit() {
        modified &= ~LINK_CREDIT;
        linkCredit = 0;
        return this;
    }

    public long getAvailable() {
        return available;
    }

    public Flow setAvailable(int available) {
        modified |= AVAILABLE;
        this.available = Integer.toUnsignedLong(available);
        return this;
    }

    public Flow setAvailable(long available) {
        if (available < 0 || available > UINT_MAX) {
            throw new IllegalArgumentException("Available value given is out of range: " + available);
        } else {
            modified |= AVAILABLE;
        }

        this.available = available;
        return this;
    }

    public Flow clearAvailable() {
        modified &= ~AVAILABLE;
        available = 0;
        return this;
    }

    public boolean getDrain() {
        return drain;
    }

    public Flow setDrain(boolean drain) {
        this.modified |= DRAIN;
        this.drain = drain;
        return this;
    }

    public Flow clearDrain() {
        modified &= ~DRAIN;
        drain = false;
        return this;
    }

    public boolean getEcho() {
        return echo;
    }

    public Flow setEcho(boolean echo) {
        this.modified |= ECHO;
        this.echo = echo;
        return this;
    }

    public Flow clearEcho() {
        modified &= ~ECHO;
        echo = false;
        return this;
    }

    public Map<Symbol, Object> getProperties() {
        return properties;
    }

    public Flow setProperties(Map<Symbol, Object> properties) {
        if (properties != null) {
            modified |= PROPERTIES;
        } else {
            modified &= ~PROPERTIES;
        }

        this.properties = properties;
        return this;
    }

    public Flow clearProperties() {
        modified &= ~PROPERTIES;
        properties = null;
        return this;
    }

    @Override
    public Flow copy() {
        Flow copy = new Flow();

        copy.nextIncomingId = nextIncomingId;
        copy.incomingWindow = incomingWindow;
        copy.nextOutgoingId = nextOutgoingId;
        copy.outgoingWindow = outgoingWindow;
        copy.handle = handle;
        copy.deliveryCount = deliveryCount;
        copy.linkCredit = linkCredit;
        copy.available = available;
        copy.drain = drain;
        copy.echo = echo;
        if (properties != null) {
            copy.properties = new LinkedHashMap<>(properties);
        }
        copy.modified = modified;

        return copy;
    }

    @Override
    public PerformativeType getPerformativeType() {
        return PerformativeType.FLOW;
    }

    @Override
    public <E> void invoke(PerformativeHandler<E> handler, ProtonBuffer payload, int channel, E context) {
        handler.handleFlow(this, payload, channel, context);
    }

    @Override
    public String toString() {
        return "Flow{" +
               "nextIncomingId=" + (hasNextIncomingId() ? nextIncomingId : "null") +
               ", incomingWindow=" + (hasIncomingWindow() ? incomingWindow : "null") +
               ", nextOutgoingId=" + (hasNextOutgoingId() ? nextOutgoingId : "null") +
               ", outgoingWindow=" + (hasOutgoingWindow() ? outgoingWindow : "null") +
               ", handle=" + (hasHandle() ? handle : "null") +
               ", deliveryCount=" + (hasDeliveryCount() ? deliveryCount : "null") +
               ", linkCredit=" + (hasLinkCredit() ? linkCredit : "null") +
               ", available=" + (hasAvailable() ? available : "null") +
               ", drain=" + (hasDrain() ? drain : "null") +
               ", echo=" + (hasEcho() ? echo : "null") +
               ", properties=" + properties +
               '}';
    }
}

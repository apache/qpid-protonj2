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

public final class Open implements Performative {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000010L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:open:list");

    private static final long UINT_MAX = 0xFFFFFFFFL;

    private static final int CONTAINER_ID = 1;
    private static final int HOSTNAME = 2;
    private static final int MAX_FRAME_SIZE = 4;
    private static final int CHANNEL_MAX = 8;
    private static final int IDLE_TIMEOUT = 16;
    private static final int OUTGOING_LOCALES = 32;
    private static final int INCOMING_LOCALES = 64;
    private static final int OFFERED_CAPABILITIES = 128;
    private static final int DESIRED_CAPABILITIES = 256;
    private static final int PROPERTIES = 512;

    private int modified = CONTAINER_ID;

    // TODO - Consider using the matching signed types instead of next largest
    //        for these values as in most cases we don't actually care about sign.
    //        In the cases we do care we could just do the math and make these
    //        interfaces simpler and not check all over the place for overflow.

    private String containerId = "";
    private String hostname;
    private long maxFrameSize = UnsignedInteger.MAX_VALUE.longValue();
    private int channelMax = UnsignedShort.MAX_VALUE.intValue();
    private long idleTimeout;
    private Symbol[] outgoingLocales;
    private Symbol[] incomingLocales;
    private Symbol[] offeredCapabilities;
    private Symbol[] desiredCapabilities;
    private Map<Symbol, Object> properties;

    @Override
    public Open copy() {
        Open copy = new Open();

        copy.setContainerId(containerId);
        if (hasHostname()) {
            copy.setHostname(hostname);
        }
        if (hasMaxFrameSize()) {
            copy.setMaxFrameSize(maxFrameSize);
        }
        if (hasChannelMax()) {
            copy.setChannelMax(channelMax);
        }
        if (hasIdleTimeout()) {
            copy.setIdleTimeout(idleTimeout);
        }
        if (hasOutgoingLocales()) {
            copy.setOutgoingLocales(Arrays.copyOf(outgoingLocales, outgoingLocales.length));
        }
        if (hasIncomingLocales()) {
            copy.setIncomingLocales(Arrays.copyOf(incomingLocales, incomingLocales.length));
        }
        if (hasOfferedCapabilities()) {
            copy.setOfferedCapabilities(Arrays.copyOf(offeredCapabilities, offeredCapabilities.length));
        }
        if (hasDesiredCapabilities()) {
            copy.setOfferedCapabilities(Arrays.copyOf(desiredCapabilities, desiredCapabilities.length));
        }
        if (hasProperties()) {
            copy.setProperties(new LinkedHashMap<>(properties));
        }

        return copy;
    }

    //----- Query the state of the Header object -----------------------------//

    public boolean isEmpty() {
        return modified == 0;
    }

    public int getElementCount() {
        return 32 - Integer.numberOfLeadingZeros(modified);
    }

    public boolean hasContainerId() {
        return (modified & CONTAINER_ID) == CONTAINER_ID;
    }

    public boolean hasHostname() {
        return (modified & HOSTNAME) == HOSTNAME;
    }

    public boolean hasMaxFrameSize() {
        return (modified & MAX_FRAME_SIZE) == MAX_FRAME_SIZE;
    }

    public boolean hasChannelMax() {
        return (modified & CHANNEL_MAX) == CHANNEL_MAX;
    }

    public boolean hasIdleTimeout() {
        return (modified & IDLE_TIMEOUT) == IDLE_TIMEOUT;
    }

    public boolean hasOutgoingLocales() {
        return (modified & OUTGOING_LOCALES) == OUTGOING_LOCALES;
    }

    public boolean hasIncomingLocales() {
        return (modified & INCOMING_LOCALES) == INCOMING_LOCALES;
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

    public String getContainerId() {
        return containerId;
    }

    public Open setContainerId(String containerId) {
        if (containerId == null) {
            throw new NullPointerException("the container-id field is mandatory");
        }

        modified |= CONTAINER_ID;

        this.containerId = containerId;
        return this;
    }

    public String getHostname() {
        return hostname;
    }

    public Open setHostname(String hostname) {
        if (hostname == null) {
            modified &= ~HOSTNAME;
        } else {
            modified |= HOSTNAME;
        }

        this.hostname = hostname;
        return this;
    }

    public long getMaxFrameSize() {
        return maxFrameSize;
    }

    public Open setMaxFrameSize(int maxFrameSize) {
        modified |= MAX_FRAME_SIZE;
        this.maxFrameSize = Integer.toUnsignedLong(maxFrameSize);
        return this;
    }

    public Open setMaxFrameSize(long maxFrameSize) {
        if (maxFrameSize < 0 || maxFrameSize > UINT_MAX) {
            throw new IllegalArgumentException(String.format(
                "Given max frame size value %d larger than this implementations limit of %d",
                maxFrameSize, Integer.MAX_VALUE));
        } else {
            modified |= MAX_FRAME_SIZE;
        }

        this.maxFrameSize = maxFrameSize;
        return this;
    }

    public int getChannelMax() {
        return channelMax;
    }

    public Open setChannelMax(short channelMax) {
        modified |= CHANNEL_MAX;
        this.channelMax = Short.toUnsignedInt(channelMax);
        return this;
    }

    public Open setChannelMax(int channelMax) {
        if (channelMax < 0 || channelMax > UnsignedShort.MAX_VALUE.intValue()) {
            throw new IllegalArgumentException("The Channel Max value given is out of range: " + channelMax);
        } else {
            modified |= CHANNEL_MAX;
        }

        this.channelMax = channelMax;
        return this;
    }

    public long getIdleTimeout() {
        return idleTimeout;
    }

    public Open setIdleTimeout(int idleTimeOut) {
        modified |= IDLE_TIMEOUT;
        this.idleTimeout = Integer.toUnsignedLong(idleTimeOut);
        return this;
    }

    public Open setIdleTimeout(long idleTimeOut) {
        if (idleTimeOut < 0 || idleTimeOut > UINT_MAX) {
            throw new IllegalArgumentException("The Idle Timeout value given is out of range: " + idleTimeOut);
        } else {
            modified |= IDLE_TIMEOUT;
        }

        this.idleTimeout = idleTimeOut;
        return this;
    }

    public Symbol[] getOutgoingLocales() {
        return outgoingLocales;
    }

    public Open setOutgoingLocales(Symbol... outgoingLocales) {
        if (outgoingLocales != null) {
            modified |= OUTGOING_LOCALES;
        } else {
            modified &= ~OUTGOING_LOCALES;
        }

        this.outgoingLocales = outgoingLocales;
        return this;
    }

    public Symbol[] getIncomingLocales() {
        return incomingLocales;
    }

    public Open setIncomingLocales(Symbol... incomingLocales) {
        if (incomingLocales != null) {
            modified |= INCOMING_LOCALES;
        } else {
            modified &= ~INCOMING_LOCALES;
        }

        this.incomingLocales = incomingLocales;
        return this;
    }

    public Symbol[] getOfferedCapabilities() {
        return offeredCapabilities;
    }

    public Open setOfferedCapabilities(Symbol... offeredCapabilities) {
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

    public Open setDesiredCapabilities(Symbol... desiredCapabilities) {
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

    public Open setProperties(Map<Symbol, Object> properties) {
        if (properties != null) {
            modified |= PROPERTIES;
        } else {
            modified &= ~PROPERTIES;
        }

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
               ", maxFrameSize=" + (hasMaxFrameSize() ? maxFrameSize : "null") +
               ", channelMax=" + (hasChannelMax() ? channelMax : "null") +
               ", idleTimeOut=" + (hasIdleTimeout() ? idleTimeout : "null") +
               ", outgoingLocales=" + (outgoingLocales == null ? "null" : Arrays.asList(outgoingLocales)) +
               ", incomingLocales=" + (incomingLocales == null ? "null" : Arrays.asList(incomingLocales)) +
               ", offeredCapabilities=" + (offeredCapabilities == null ? "null" : Arrays.asList(offeredCapabilities)) +
               ", desiredCapabilities=" + (desiredCapabilities == null ? "null" : Arrays.asList(desiredCapabilities)) +
               ", properties=" + properties +
               '}';
    }
}

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

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.messaging.Source;
import org.apache.qpid.proton4j.amqp.messaging.Target;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;

public final class Attach implements Performative {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000012L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:attach:list");

    private static final long UINT_MAX = 0xFFFFFFFFL;

    private static int NAME = 1;
    private static int HANDLE = 2;
    private static int ROLE = 4;
    private static int SENDER_SETTLE_MODE = 8;
    private static int RECEIVER_SETTLE_MODE = 16;
    private static int SOURCE = 32;
    private static int TARGET = 64;
    private static int UNSETTLED = 128;
    private static int INCOMPLETE_UNSETTLED = 256;
    private static int INITIAL_DELIVERY_COUNT = 512;
    private static int MAX_MESSAGE_SIZE = 1024;
    private static int OFFERED_CAPABILITIES = 2048;
    private static int DESIRED_CAPABILITIES = 4096;
    private static int PROPERTIES = 8192;

    private int modified = 0;

    private String name;
    private long handle;
    private Role role = Role.SENDER;
    private SenderSettleMode sndSettleMode = SenderSettleMode.MIXED;
    private ReceiverSettleMode rcvSettleMode = ReceiverSettleMode.FIRST;
    private Source source;
    private Target target;
    private Map<Binary, DeliveryState> unsettled;
    private boolean incompleteUnsettled;
    private long initialDeliveryCount;
    private UnsignedLong maxMessageSize;
    private Symbol[] offeredCapabilities;
    private Symbol[] desiredCapabilities;
    private Map<Symbol, Object> properties;

    @Override
    public PerformativeType getPerformativeType() {
        return PerformativeType.ATTACH;
    }

    @Override
    public Attach copy() {
        Attach copy = new Attach();

        copy.name = name;
        copy.handle = handle;
        copy.role = role;
        copy.sndSettleMode = sndSettleMode;
        copy.rcvSettleMode = rcvSettleMode;
        copy.source = source;
        copy.target = target;
        if (unsettled != null) {
            copy.unsettled = new LinkedHashMap<>(unsettled);
        }
        copy.incompleteUnsettled = incompleteUnsettled;
        copy.initialDeliveryCount = initialDeliveryCount;
        copy.maxMessageSize = maxMessageSize;
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

    public boolean hasName() {
        return (modified & NAME) == NAME;
    }

    public boolean hasHandle() {
        return (modified & HANDLE) == HANDLE;
    }

    public boolean hasRole() {
        return (modified & ROLE) == ROLE;
    }

    public boolean hasSenderSettleMode() {
        return (modified & SENDER_SETTLE_MODE) == SENDER_SETTLE_MODE;
    }

    public boolean hasReceiverSettleMode() {
        return (modified & RECEIVER_SETTLE_MODE) == RECEIVER_SETTLE_MODE;
    }

    public boolean hasSource() {
        return (modified & SOURCE) == SOURCE;
    }

    public boolean hasTarget() {
        return (modified & TARGET) == TARGET;
    }

    public boolean hasUnsettled() {
        return (modified & UNSETTLED) == UNSETTLED;
    }

    public boolean hasIncompleteUnsettled() {
        return (modified & INCOMPLETE_UNSETTLED) == INCOMPLETE_UNSETTLED;
    }

    public boolean hasInitialDeliveryCount() {
        return (modified & INITIAL_DELIVERY_COUNT) == INITIAL_DELIVERY_COUNT;
    }

    public boolean hasMaxMessageSize() {
        return (modified & MAX_MESSAGE_SIZE) == MAX_MESSAGE_SIZE;
    }

    public boolean hasOfferedCapabilites() {
        return (modified & OFFERED_CAPABILITIES) == OFFERED_CAPABILITIES;
    }

    public boolean hasDesiredCapabilites() {
        return (modified & DESIRED_CAPABILITIES) == DESIRED_CAPABILITIES;
    }

    public boolean hasProperties() {
        return (modified & PROPERTIES) == PROPERTIES;
    }

    //----- Access to the member data with state checks

    public String getName() {
        return name;
    }

    public void setName(String name) {
        if (name == null) {
            throw new NullPointerException("the name field is mandatory");
        }

        modified |= NAME;

        this.name = name;
    }

    public long getHandle() {
        return handle;
    }

    public void setHandle(long handle) {
        if (handle < 0 || handle > UINT_MAX) {
            throw new IllegalArgumentException("The Handle value given is out of range: " + handle);
        } else {
            modified |= HANDLE;
        }

        this.handle = handle;
    }

    public Role getRole() {
        return role;
    }

    public void setRole(Role role) {
        if (role == null) {
            throw new NullPointerException("Role cannot be null");
        }

        modified |= ROLE;

        this.role = role;
    }

    public SenderSettleMode getSndSettleMode() {
        return sndSettleMode;
    }

    public void setSndSettleMode(SenderSettleMode sndSettleMode) {
        if (sndSettleMode != null) {
            modified |= SENDER_SETTLE_MODE;
        } else {
            modified &= ~SENDER_SETTLE_MODE;
        }

        this.sndSettleMode = sndSettleMode == null ? SenderSettleMode.MIXED : sndSettleMode;
    }

    public ReceiverSettleMode getRcvSettleMode() {
        return rcvSettleMode;
    }

    public void setRcvSettleMode(ReceiverSettleMode rcvSettleMode) {
        if (rcvSettleMode != null) {
            modified |= RECEIVER_SETTLE_MODE;
        } else {
            modified &= ~RECEIVER_SETTLE_MODE;
        }

        this.rcvSettleMode = rcvSettleMode == null ? ReceiverSettleMode.FIRST : rcvSettleMode;
    }

    public Source getSource() {
        return source;
    }

    public void setSource(Source source) {
        if (source != null) {
            modified |= SOURCE;
        } else {
            modified &= ~SOURCE;
        }

        this.source = source;
    }

    public Target getTarget() {
        return target;
    }

    public void setTarget(Target target) {
        if (target != null) {
            modified |= TARGET;
        } else {
            modified &= ~TARGET;
        }

        this.target = target;
    }

    public Map<Binary, DeliveryState> getUnsettled() {
        return unsettled;
    }

    public void setUnsettled(Map<Binary, DeliveryState> unsettled) {
        if (unsettled != null) {
            modified |= UNSETTLED;
        } else {
            modified &= ~UNSETTLED;
        }

        this.unsettled = unsettled;
    }

    public boolean getIncompleteUnsettled() {
        return incompleteUnsettled;
    }

    public void setIncompleteUnsettled(boolean incompleteUnsettled) {
        modified |= INCOMPLETE_UNSETTLED;

        this.incompleteUnsettled = incompleteUnsettled;
    }

    public long getInitialDeliveryCount() {
        return initialDeliveryCount;
    }

    public void setInitialDeliveryCount(long initialDeliveryCount) {
        if (handle < 0 || handle > UINT_MAX) {
            throw new IllegalArgumentException("The initial delivery count value given is out of range: " + handle);
        } else {
            modified |= INITIAL_DELIVERY_COUNT;
        }

        this.initialDeliveryCount = initialDeliveryCount;
    }

    public UnsignedLong getMaxMessageSize() {
        return maxMessageSize;
    }

    public void setMaxMessageSize(UnsignedLong maxMessageSize) {
        if (offeredCapabilities != null) {
            modified |= MAX_MESSAGE_SIZE;
        } else {
            modified &= ~MAX_MESSAGE_SIZE;
        }

        this.maxMessageSize = maxMessageSize;
    }

    public Symbol[] getOfferedCapabilities() {
        return offeredCapabilities;
    }

    public void setOfferedCapabilities(Symbol... offeredCapabilities) {
        if (offeredCapabilities != null) {
            modified |= OFFERED_CAPABILITIES;
        } else {
            modified &= ~OFFERED_CAPABILITIES;
        }

        this.offeredCapabilities = offeredCapabilities;
    }

    public Symbol[] getDesiredCapabilities() {
        return desiredCapabilities;
    }

    public void setDesiredCapabilities(Symbol... desiredCapabilities) {
        if (desiredCapabilities != null) {
            modified |= DESIRED_CAPABILITIES;
        } else {
            modified &= ~DESIRED_CAPABILITIES;
        }

        this.desiredCapabilities = desiredCapabilities;
    }

    public Map<Symbol, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<Symbol, Object> properties) {
        if (properties != null) {
            modified |= PROPERTIES;
        } else {
            modified &= ~PROPERTIES;
        }

        this.properties = properties;
    }

    @Override
    public <E> void invoke(PerformativeHandler<E> handler, ProtonBuffer payload, int channel, E context) {
        handler.handleAttach(this, payload, channel, context);
    }

    @Override
    public String toString() {
        return "Attach{" +
            "name='" + name + '\'' +
            ", handle=" + handle +
            ", role=" + role +
            ", sndSettleMode=" + sndSettleMode +
            ", rcvSettleMode=" + rcvSettleMode +
            ", source=" + source +
            ", target=" + target +
            ", unsettled=" + unsettled +
            ", incompleteUnsettled=" + incompleteUnsettled +
            ", initialDeliveryCount=" + initialDeliveryCount +
            ", maxMessageSize=" + maxMessageSize +
            ", offeredCapabilities=" + (offeredCapabilities == null ? null : Arrays.asList(offeredCapabilities)) +
            ", desiredCapabilities=" + (desiredCapabilities == null ? null : Arrays.asList(desiredCapabilities)) +
            ", properties=" + properties + '}';
    }
}

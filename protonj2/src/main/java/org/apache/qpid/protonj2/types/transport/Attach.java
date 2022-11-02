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
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.messaging.Source;
import org.apache.qpid.protonj2.types.messaging.Target;
import org.apache.qpid.protonj2.types.messaging.Terminus;
import org.apache.qpid.protonj2.types.transactions.Coordinator;

public final class Attach implements Performative {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000012L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:attach:list");

    private static final long UINT_MAX = 0xFFFFFFFFL;

    private static final int NAME = 1;
    private static final int HANDLE = 2;
    private static final int ROLE = 4;
    private static final int SENDER_SETTLE_MODE = 8;
    private static final int RECEIVER_SETTLE_MODE = 16;
    private static final int SOURCE = 32;
    private static final int TARGET = 64;
    private static final int UNSETTLED = 128;
    private static final int INCOMPLETE_UNSETTLED = 256;
    private static final int INITIAL_DELIVERY_COUNT = 512;
    private static final int MAX_MESSAGE_SIZE = 1024;
    private static final int OFFERED_CAPABILITIES = 2048;
    private static final int DESIRED_CAPABILITIES = 4096;
    private static final int PROPERTIES = 8192;

    private int modified = 0;

    // TODO - Consider using the matching signed types instead of next largest
    //        for these values as in most cases we don't actually care about sign.
    //        In the cases we do care we could just do the math and make these
    //        interfaces simpler and not check all over the place for overflow.

    private String name;
    private long handle;
    private Role role = Role.SENDER;
    private SenderSettleMode sndSettleMode = SenderSettleMode.MIXED;
    private ReceiverSettleMode rcvSettleMode = ReceiverSettleMode.FIRST;
    private Source source;
    private Terminus target;
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

    public boolean hasElement(int index) {
        final int value = 1 << index;
        return (modified & value) == value;
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

    public boolean hasTargetOrCoordinator() {
        return (modified & TARGET) == TARGET;
    }

    public boolean hasTarget() {
        return (modified & TARGET) == TARGET && target instanceof Target;
    }

    public boolean hasCoordinator() {
        return (modified & TARGET) == TARGET && target instanceof Coordinator;
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

    public String getName() {
        return name;
    }

    public Attach setName(String name) {
        if (name == null) {
            throw new NullPointerException("the name field is mandatory");
        }

        modified |= NAME;

        this.name = name;
        return this;
    }

    public long getHandle() {
        return handle;
    }

    public Attach setHandle(int handle) {
        modified |= HANDLE;
        this.handle = Integer.toUnsignedLong(handle);
        return this;
    }

    public Attach setHandle(long handle) {
        if (handle < 0 || handle > UINT_MAX) {
            throw new IllegalArgumentException("The Handle value given is out of range: " + handle);
        } else {
            modified |= HANDLE;
        }

        this.handle = handle;
        return this;
    }

    public Role getRole() {
        return role;
    }

    public Attach setRole(Role role) {
        if (role == null) {
            throw new NullPointerException("Role cannot be null");
        }

        modified |= ROLE;

        this.role = role;
        return this;
    }

    public SenderSettleMode getSenderSettleMode() {
        return sndSettleMode;
    }

    public Attach setSenderSettleMode(SenderSettleMode sndSettleMode) {
        if (sndSettleMode != null) {
            modified |= SENDER_SETTLE_MODE;
        } else {
            modified &= ~SENDER_SETTLE_MODE;
        }

        this.sndSettleMode = sndSettleMode == null ? SenderSettleMode.MIXED : sndSettleMode;
        return this;
    }

    public ReceiverSettleMode getReceiverSettleMode() {
        return rcvSettleMode;
    }

    public Attach setReceiverSettleMode(ReceiverSettleMode rcvSettleMode) {
        if (rcvSettleMode != null) {
            modified |= RECEIVER_SETTLE_MODE;
        } else {
            modified &= ~RECEIVER_SETTLE_MODE;
        }

        this.rcvSettleMode = rcvSettleMode == null ? ReceiverSettleMode.FIRST : rcvSettleMode;
        return this;
    }

    public Source getSource() {
        return source;
    }

    public Attach setSource(Source source) {
        if (source != null) {
            modified |= SOURCE;
        } else {
            modified &= ~SOURCE;
        }

        this.source = source;
        return this;
    }

    @SuppressWarnings("unchecked")
    public <T extends Terminus> T getTarget() {
        return (T) target;
    }

    public Attach setTarget(Terminus target) {
        if (target instanceof Target) {
            setTarget((Target) target);
        } else if (target instanceof Coordinator) {
            setTarget((Coordinator) target);
        } else {
            throw new IllegalArgumentException("Cannot set Target terminus to given value: " + target);
        }

        return this;
    }

    public Attach setTarget(Target target) {
        if (target != null) {
            modified |= TARGET;
        } else {
            modified &= ~TARGET;
        }

        this.target = target;
        return this;
    }

    public Attach setTarget(Coordinator target) {
        if (target != null) {
            modified |= TARGET;
        } else {
            modified &= ~TARGET;
        }

        this.target = target;
        return this;
    }

    public Attach setCoordinator(Coordinator target) {
        if (target != null) {
            modified |= TARGET;
        } else {
            modified &= ~TARGET;
        }

        this.target = target;
        return this;
    }

    public Map<Binary, DeliveryState> getUnsettled() {
        return unsettled;
    }

    public Attach setUnsettled(Map<Binary, DeliveryState> unsettled) {
        if (unsettled != null) {
            modified |= UNSETTLED;
        } else {
            modified &= ~UNSETTLED;
        }

        this.unsettled = unsettled;
        return this;
    }

    public boolean getIncompleteUnsettled() {
        return incompleteUnsettled;
    }

    public Attach setIncompleteUnsettled(boolean incompleteUnsettled) {
        this.modified |= INCOMPLETE_UNSETTLED;
        this.incompleteUnsettled = incompleteUnsettled;
        return this;
    }

    public long getInitialDeliveryCount() {
        return initialDeliveryCount;
    }

    public Attach setInitialDeliveryCount(int initialDeliveryCount) {
        modified |= INITIAL_DELIVERY_COUNT;
        this.initialDeliveryCount = Integer.toUnsignedLong(initialDeliveryCount);
        return this;
    }

    public Attach setInitialDeliveryCount(long initialDeliveryCount) {
        if (initialDeliveryCount < 0 || initialDeliveryCount > UINT_MAX) {
            throw new IllegalArgumentException("The initial delivery count value given is out of range: " + handle);
        } else {
            modified |= INITIAL_DELIVERY_COUNT;
        }

        this.initialDeliveryCount = initialDeliveryCount;
        return this;
    }

    public UnsignedLong getMaxMessageSize() {
        return maxMessageSize;
    }

    public Attach setMaxMessageSize(long maxMessageSize) {
        return setMaxMessageSize(UnsignedLong.valueOf(maxMessageSize));
    }

    public Attach setMaxMessageSize(UnsignedLong maxMessageSize) {
        if (maxMessageSize != null) {
            modified |= MAX_MESSAGE_SIZE;
        } else {
            modified &= ~MAX_MESSAGE_SIZE;
        }

        this.maxMessageSize = maxMessageSize;
        return this;
    }

    public Symbol[] getOfferedCapabilities() {
        return offeredCapabilities;
    }

    public Attach setOfferedCapabilities(Symbol... offeredCapabilities) {
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

    public Attach setDesiredCapabilities(Symbol... desiredCapabilities) {
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

    public Attach setProperties(Map<Symbol, Object> properties) {
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
        handler.handleAttach(this, payload, channel, context);
    }

    @Override
    public String toString() {
        return "Attach{" +
            "name='" + name + '\'' +
            ", handle=" + (hasHandle() ? handle : "null") +
            ", role=" + (hasRole() ? role : "null") +
            ", sndSettleMode=" + (hasSenderSettleMode() ? sndSettleMode : "null") +
            ", rcvSettleMode=" + (hasReceiverSettleMode() ? rcvSettleMode : "null") +
            ", source=" + source +
            ", target=" + target +
            ", unsettled=" + unsettled +
            ", incompleteUnsettled=" + (hasIncompleteUnsettled() ? incompleteUnsettled : "null") +
            ", initialDeliveryCount=" + (hasInitialDeliveryCount() ? initialDeliveryCount : "null") +
            ", maxMessageSize=" + maxMessageSize +
            ", offeredCapabilities=" + (offeredCapabilities == null ? "null" : Arrays.asList(offeredCapabilities)) +
            ", desiredCapabilities=" + (desiredCapabilities == null ? "null" : Arrays.asList(desiredCapabilities)) +
            ", properties=" + properties + '}';
    }
}

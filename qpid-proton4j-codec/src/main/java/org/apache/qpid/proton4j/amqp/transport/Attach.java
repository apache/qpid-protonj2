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
import java.util.Map;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.messaging.Source;
import org.apache.qpid.proton4j.amqp.messaging.Target;

public final class Attach implements Performative {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000012L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:attach:list");

    private String name;
    private UnsignedInteger handle;
    private Role role = Role.SENDER;
    private SenderSettleMode sndSettleMode = SenderSettleMode.MIXED;
    private ReceiverSettleMode rcvSettleMode = ReceiverSettleMode.FIRST;
    private Source source;
    private Target target;
    private Map<Binary, DeliveryState> unsettled;
    private boolean incompleteUnsettled;
    private UnsignedInteger initialDeliveryCount;
    private UnsignedLong maxMessageSize;
    private Symbol[] offeredCapabilities;
    private Symbol[] desiredCapabilities;
    private Map<Object, Object> properties;

    @Override
    public PerformativeType getPerformativeType() {
        return PerformativeType.ATTACH;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        if (name == null) {
            throw new NullPointerException("the name field is mandatory");
        }

        this.name = name;
    }

    public UnsignedInteger getHandle() {
        return handle;
    }

    public void setHandle(UnsignedInteger handle) {
        if (handle == null) {
            throw new NullPointerException("the handle field is mandatory");
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
        this.role = role;
    }

    public SenderSettleMode getSndSettleMode() {
        return sndSettleMode;
    }

    public void setSndSettleMode(SenderSettleMode sndSettleMode) {
        this.sndSettleMode = sndSettleMode == null ? SenderSettleMode.MIXED : sndSettleMode;
    }

    public ReceiverSettleMode getRcvSettleMode() {
        return rcvSettleMode;
    }

    public void setRcvSettleMode(ReceiverSettleMode rcvSettleMode) {
        this.rcvSettleMode = rcvSettleMode == null ? ReceiverSettleMode.FIRST : rcvSettleMode;
    }

    public Source getSource() {
        return source;
    }

    public void setSource(Source source) {
        this.source = source;
    }

    public Target getTarget() {
        return target;
    }

    public void setTarget(Target target) {
        this.target = target;
    }

    public Map<Binary, DeliveryState> getUnsettled() {
        return unsettled;
    }

    public void setUnsettled(Map<Binary, DeliveryState> unsettled) {
        this.unsettled = unsettled;
    }

    public boolean getIncompleteUnsettled() {
        return incompleteUnsettled;
    }

    public void setIncompleteUnsettled(boolean incompleteUnsettled) {
        this.incompleteUnsettled = incompleteUnsettled;
    }

    public UnsignedInteger getInitialDeliveryCount() {
        return initialDeliveryCount;
    }

    public void setInitialDeliveryCount(UnsignedInteger initialDeliveryCount) {
        this.initialDeliveryCount = initialDeliveryCount;
    }

    public UnsignedLong getMaxMessageSize() {
        return maxMessageSize;
    }

    public void setMaxMessageSize(UnsignedLong maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
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
    public <E> void invoke(PerformativeHandler<E> handler, Binary payload, E context) {
        handler.handleAttach(this, payload, context);
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

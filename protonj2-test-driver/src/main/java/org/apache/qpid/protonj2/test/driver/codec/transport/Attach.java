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
package org.apache.qpid.protonj2.test.driver.codec.transport;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.qpid.protonj2.test.driver.codec.messaging.Source;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Target;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Binary;
import org.apache.qpid.protonj2.test.driver.codec.primitives.DescribedType;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Symbol;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedByte;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedInteger;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedLong;
import org.apache.qpid.protonj2.test.driver.codec.transactions.Coordinator;

import io.netty5.buffer.Buffer;

public class Attach extends PerformativeDescribedType {

    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:attach:list");
    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000012L);

    /**
     * Enumeration which maps to fields in the Attach Performative
     */
    public enum Field {
        NAME,
        HANDLE,
        ROLE,
        SND_SETTLE_MODE,
        RCV_SETTLE_MODE,
        SOURCE,
        TARGET,
        UNSETTLED,
        INCOMPLETE_UNSETTLED,
        INITIAL_DELIVERY_COUNT,
        MAX_MESSAGE_SIZE,
        OFFERED_CAPABILITIES,
        DESIRED_CAPABILITIES,
        PROPERTIES
    }

    public Attach() {
        super(Field.values().length);
    }

    @SuppressWarnings("unchecked")
    public Attach(Object described) {
        super(Field.values().length, (List<Object>) described);
    }

    public Attach(List<Object> described) {
        super(Field.values().length, described);
    }

    public boolean isSender() {
        return getRole().booleanValue() == false;
    }

    public boolean isReceiver() {
        return getRole().booleanValue() == true;
    }

    @Override
    public Symbol getDescriptor() {
        return DESCRIPTOR_SYMBOL;
    }

    public Attach setName(String o) {
        getList().set(Field.NAME.ordinal(), o);
        return this;
    }

    public String getName() {
        return (String) getList().get(Field.NAME.ordinal());
    }

    public Attach setHandle(UnsignedInteger o) {
        getList().set(Field.HANDLE.ordinal(), o);
        return this;
    }

    public UnsignedInteger getHandle() {
        return (UnsignedInteger) getList().get(Field.HANDLE.ordinal());
    }

    public Attach setRole(Boolean o) {
        getList().set(Field.ROLE.ordinal(), o);
        return this;
    }

    public Attach setRole(boolean o) {
        getList().set(Field.ROLE.ordinal(), o);
        return this;
    }

    public Attach setRole(Role role) {
        getList().set(Field.ROLE.ordinal(), role.getValue());
        return this;
    }

    public Boolean getRole() {
        return (Boolean) getList().get(Field.ROLE.ordinal());
    }

    public Attach setSenderSettleMode(byte o) {
        getList().set(Field.SND_SETTLE_MODE.ordinal(), UnsignedByte.valueOf(o));
        return this;
    }

    public Attach setSenderSettleMode(UnsignedByte o) {
        getList().set(Field.SND_SETTLE_MODE.ordinal(), o);
        return this;
    }

    public Attach setSenderSettleMode(SenderSettleMode o) {
        getList().set(Field.SND_SETTLE_MODE.ordinal(), o.getValue());
        return this;
    }

    public UnsignedByte getSenderSettleMode() {
        return (UnsignedByte) getList().get(Field.SND_SETTLE_MODE.ordinal());
    }

    public Attach setReceiverSettleMode(byte o) {
        getList().set(Field.RCV_SETTLE_MODE.ordinal(), UnsignedByte.valueOf(o));
        return this;
    }

    public Attach setReceiverSettleMode(UnsignedByte o) {
        getList().set(Field.RCV_SETTLE_MODE.ordinal(), o);
        return this;
    }

    public Attach setReceiverSettleMode(ReceiverSettleMode o) {
        getList().set(Field.RCV_SETTLE_MODE.ordinal(), o.getValue());
        return this;
    }

    public UnsignedByte getReceiverSettleMode() {
        return (UnsignedByte) getList().get(Field.RCV_SETTLE_MODE.ordinal());
    }

    public Attach setSource(Source o) {
        getList().set(Field.SOURCE.ordinal(), o);
        return this;
    }

    public Source getSource() {
        return (Source) getList().get(Field.SOURCE.ordinal());
    }

    public Attach setTarget(Target o) {
        getList().set(Field.TARGET.ordinal(), o);
        return this;
    }

    public Attach setTarget(Coordinator o) {
        getList().set(Field.TARGET.ordinal(), o);
        return this;
    }

    public Object getTarget() {
        return getList().get(Field.TARGET.ordinal());
    }

    public Attach setUnsettled(Map<Binary, DescribedType> o) {
        getList().set(Field.UNSETTLED.ordinal(), o);
        return this;
    }

    @SuppressWarnings("unchecked")
    public Map<Binary, DescribedType> getUnsettled() {
        return (Map<Binary, DescribedType>) getList().get(Field.UNSETTLED.ordinal());
    }

    public Attach setIncompleteUnsettled(Boolean o) {
        getList().set(Field.INCOMPLETE_UNSETTLED.ordinal(), o);
        return this;
    }

    public Boolean getIncompleteUnsettled() {
        return (Boolean) getList().get(Field.INCOMPLETE_UNSETTLED.ordinal());
    }

    public Attach setInitialDeliveryCount(UnsignedInteger o) {
        getList().set(Field.INITIAL_DELIVERY_COUNT.ordinal(), o);
        return this;
    }

    public UnsignedInteger getInitialDeliveryCount() {
        return (UnsignedInteger) getList().get(Field.INITIAL_DELIVERY_COUNT.ordinal());
    }

    public Attach setMaxMessageSize(UnsignedLong o) {
        getList().set(Field.MAX_MESSAGE_SIZE.ordinal(), o);
        return this;
    }

    public UnsignedLong getMaxMessageSize() {
        return (UnsignedLong) getList().get(Field.MAX_MESSAGE_SIZE.ordinal());
    }

    public Attach setOfferedCapabilities(Symbol[] o) {
        getList().set(Field.OFFERED_CAPABILITIES.ordinal(), o);
        return this;
    }

    public Symbol[] getOfferedCapabilities() {
        return (Symbol[]) getList().get(Field.OFFERED_CAPABILITIES.ordinal());
    }

    public Attach setDesiredCapabilities(Symbol[] o) {
        getList().set(Field.DESIRED_CAPABILITIES.ordinal(), o);
        return this;
    }

    public Symbol[] getDesiredCapabilities() {
        return (Symbol[]) getList().get(Field.DESIRED_CAPABILITIES.ordinal());
    }

    public Attach setProperties(Map<Symbol, Object> o) {
        getList().set(Field.PROPERTIES.ordinal(), o);
        return this;
    }

    @SuppressWarnings("unchecked")
    public Map<Symbol, Object> getProperties() {
        return (Map<Symbol, Object>) getList().get(Field.PROPERTIES.ordinal());
    }

    @Override
    public PerformativeType getPerformativeType() {
        return PerformativeType.ATTACH;
    }

    @Override
    public <E> void invoke(PerformativeHandler<E> handler, int frameSize, Buffer payload, int channel, E context) {
        handler.handleAttach(frameSize, this, payload, channel, context);
    }

    @Override
    public Object getFieldValueOrSpecDefault(int index) {
        Object result = getFieldValue(index);
        if (result == null) {
            Field field = Field.values()[index];
            switch (field) {
                case SND_SETTLE_MODE:
                    result = SenderSettleMode.MIXED;
                    break;
                case RCV_SETTLE_MODE:
                    result = ReceiverSettleMode.FIRST;
                    break;
                case INCOMPLETE_UNSETTLED:
                    result = Boolean.FALSE;
                    break;
                default:
                    break;
            }
        }
        return result;
    }

    @Override
    public String toString() {
        return "Attach{" +
            "name='" + getName() + '\'' +
            ", handle=" + getHandle() +
            ", role=" + getRole() +
            ", sndSettleMode=" + getSenderSettleMode() +
            ", rcvSettleMode=" + getReceiverSettleMode() +
            ", source=" + getSource() +
            ", target=" + getTarget() +
            ", unsettled=" + getUnsettled() +
            ", incompleteUnsettled=" + getIncompleteUnsettled() +
            ", initialDeliveryCount=" + getInitialDeliveryCount() +
            ", maxMessageSize=" + getMaxMessageSize() +
            ", offeredCapabilities=" + Arrays.toString(getOfferedCapabilities()) +
            ", desiredCapabilities=" + Arrays.toString(getDesiredCapabilities()) +
            ", properties=" + getProperties() + '}';
    }
}

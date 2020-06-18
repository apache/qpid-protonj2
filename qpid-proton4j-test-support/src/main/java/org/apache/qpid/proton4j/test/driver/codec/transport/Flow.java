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
package org.apache.qpid.proton4j.test.driver.codec.transport;

import java.util.List;
import java.util.Map;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.types.Symbol;
import org.apache.qpid.proton4j.types.UnsignedInteger;
import org.apache.qpid.proton4j.types.UnsignedLong;

public class Flow extends PerformativeDescribedType {

    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:flow:list");
    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000013L);

    /**
     * Enumeration which maps to fields in the Flow Performative
     */
    public enum Field {
        NEXT_INCOMING_ID,
        INCOMING_WINDOW,
        NEXT_OUTGOING_ID,
        OUTGOING_WINDOW,
        HANDLE,
        DELIVERY_COUNT,
        LINK_CREDIT,
        AVAILABLE,
        DRAIN,
        ECHO,
        PROPERTIES,
    }

    public Flow() {
        super(Field.values().length);
    }

    @SuppressWarnings("unchecked")
    public Flow(Object described) {
        super(Field.values().length, (List<Object>) described);
    }

    public Flow(List<Object> described) {
        super(Field.values().length, described);
    }

    @Override
    public Symbol getDescriptor() {
        return DESCRIPTOR_SYMBOL;
    }

    public Flow setNextIncomingId(UnsignedInteger o) {
        getList().set(Field.NEXT_INCOMING_ID.ordinal(), o);
        return this;
    }

    public UnsignedInteger getNextIncomingId() {
        return (UnsignedInteger) getList().get(Field.NEXT_INCOMING_ID.ordinal());
    }

    public Flow setIncomingWindow(UnsignedInteger o) {
        getList().set(Field.INCOMING_WINDOW.ordinal(), o);
        return this;
    }

    public UnsignedInteger getIncomingWindow() {
        return (UnsignedInteger) getList().get(Field.INCOMING_WINDOW.ordinal());
    }

    public Flow setNextOutgoingId(UnsignedInteger o) {
        getList().set(Field.NEXT_OUTGOING_ID.ordinal(), o);
        return this;
    }

    public UnsignedInteger getNextOutgoingId() {
        return (UnsignedInteger) getList().get(Field.NEXT_OUTGOING_ID.ordinal());
    }

    public Flow setOutgoingWindow(UnsignedInteger o) {
        getList().set(Field.OUTGOING_WINDOW.ordinal(), o);
        return this;
    }

    public UnsignedInteger getOutgoingWindow() {
        return (UnsignedInteger) getList().get(Field.OUTGOING_WINDOW.ordinal());
    }

    public Flow setHandle(UnsignedInteger o) {
        getList().set(Field.HANDLE.ordinal(), o);
        return this;
    }

    public UnsignedInteger getHandle() {
        return (UnsignedInteger) getList().get(Field.HANDLE.ordinal());
    }

    public Flow setDeliveryCount(UnsignedInteger o) {
        getList().set(Field.DELIVERY_COUNT.ordinal(), o);
        return this;
    }

    public UnsignedInteger getDeliveryCount() {
        return (UnsignedInteger) getList().get(Field.DELIVERY_COUNT.ordinal());
    }

    public Flow setLinkCredit(UnsignedInteger o) {
        getList().set(Field.LINK_CREDIT.ordinal(), o);
        return this;
    }

    public UnsignedInteger getLinkCredit() {
        return (UnsignedInteger) getList().get(Field.LINK_CREDIT.ordinal());
    }

    public Flow setAvailable(UnsignedInteger o) {
        getList().set(Field.AVAILABLE.ordinal(), o);
        return this;
    }

    public UnsignedInteger getAvailable() {
        return (UnsignedInteger) getList().get(Field.AVAILABLE.ordinal());
    }

    public Flow setDrain(Boolean o) {
        getList().set(Field.DRAIN.ordinal(), o);
        return this;
    }

    public Boolean getDrain() {
        return (Boolean) getList().get(Field.DRAIN.ordinal());
    }

    public Flow setEcho(Boolean o) {
        getList().set(Field.ECHO.ordinal(), o);
        return this;
    }

    public Boolean getEcho() {
        return (Boolean) getList().get(Field.ECHO.ordinal());
    }

    public Flow setProperties(Map<Symbol, Object> o) {
        getList().set(Field.PROPERTIES.ordinal(), o);
        return this;
    }

    @SuppressWarnings("unchecked")
    public Map<Symbol, Object> getProperties() {
        return (Map<Symbol, Object>) getList().get(Field.PROPERTIES.ordinal());
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
    public Object getFieldValueOrSpecDefault(int index) {
        Object result = getFieldValue(index);
        if (result == null) {
            Field field = Field.values()[index];
            switch (field) {
                case DRAIN:
                    result = Boolean.FALSE;
                    break;
                case ECHO:
                    result = Boolean.FALSE;
                    break;
                default:
                    break;
            }
        }
        return result;
    }
}
